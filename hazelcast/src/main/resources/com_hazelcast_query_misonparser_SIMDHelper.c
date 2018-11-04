#include "com_hazelcast_query_misonparser_SIMDHelper.h"
#include<stdio.h>
#include<string.h>
#include<emmintrin.h>
#define NLINE 64
#define E(x) ((x) & (-(x)))
#define R(x) ((x) & ((x)-1))

// : mask (16 bits each 128 bits total)
__m128i colonMask;
__m128i leftBraceMask;
__m128i rightBraceMask;
__m128i quoteMask;
__m128i backslashMask;
__m128i zeroMask;

struct stack_item {
    uint64_t val;
    int index;
};

struct stack {
    struct stack_item arr[1000];
    int pt;
};

void stack_init(struct stack *s) {
    (*s).pt = 0;
}

void stack_push(struct stack *s, struct stack_item it) {
    (*s).arr[(*s).pt++] = it;
}

struct stack_item stack_pop(struct stack *s) {
    if ((*s).pt == 0) {
        struct stack_item n;
        return n;
    }
    return (*s).arr[--(*s).pt];
}

int stack_size(struct stack *s) {
    return (*s).pt;
}

void prepareMasks() {
    colonMask = _mm_set1_epi16(58);
    leftBraceMask = _mm_set1_epi16(123);
    rightBraceMask = _mm_set1_epi16(125);
    quoteMask = _mm_set1_epi16(34);
    backslashMask = _mm_set1_epi16(92);
    zeroMask = _mm_set1_epi16(0);
}

void printLongLE(uint64_t b) {
    for (uint64_t i = 0; i < 8; i++) {
        uint64_t masked = (b & (((uint64_t)0xFF) << (i*8))) >> i*8;
        for (uint64_t j = 0; j < 8; j++) {
            printf("%c", (masked % 2 == 1) ? '+': '-');
            masked = masked >> 1;
        }
    }
}

void printLongArrayLE(uint64_t *b, int n) {
    for (int i = 0; i < n; i++) {
        printLongLE(b[i]);
    }
}

void fillIndex(
    const u_int16_t *chars,
    __m128i characterMask,
    jint numberOfBitLines,
    uint64_t* indexArray
) {
    memset(indexArray, 0, numberOfBitLines * sizeof(uint64_t));
    uint64_t i;
    for (i = 0; i < numberOfBitLines * 8; i++) {
        __m128i charLine = _mm_loadu_si128((const __m128i *)chars + i);
        __m128i res = _mm_cmpeq_epi16(charLine, characterMask);
        res = _mm_packs_epi16(res, zeroMask);

        uint64_t mask = _mm_movemask_epi8(res);

        int currentIndex = i / 8;
        // printf("%llu\n", (mask << (8 * (i%8))));
        indexArray[currentIndex] = (indexArray[currentIndex] ) | (mask << (8 * (i%8)));
    }
}

void buildLeveledColonMap(uint64_t *indexCollection, int indexMaxNesting, int n, uint64_t *colonIndex, uint64_t *lBraceIndex, uint64_t *rBraceIndex) {
    struct stack S;
    stack_init(&S);
    int stackIndex = 0;
    uint64_t i;
    for (i = 0; i < indexMaxNesting; i++) {
        int j;
        for (j = 0; j < n; j++) {
            indexCollection[i*n+j] = colonIndex[j];
        }
    }
    
    // printLongArrayLE(rBraceIndex, n);
    // printf("\n");
    for (i = 0; i < n; i++) {
        uint64_t mleft = lBraceIndex[i];
        uint64_t mright = rBraceIndex[i];
        uint64_t mrightbit = 0;
        // printf("%d\n", i);
        do {
            mrightbit = E(mright);
            uint64_t mleftbit = E(mleft);
            while (mleftbit != 0 && (mrightbit == 0 || mleftbit < mrightbit)) {
                struct stack_item it;
                it.val = mleftbit;
                it.index = i;
                stack_push(&S, it);
                mleft = R(mleft);
                mleftbit = E(mleft);
            }
            if (mrightbit != 0) {
                uint64_t stackSize = stack_size(&S);
                struct stack_item it = stack_pop(&S);
                uint64_t j = it.index;
                mleftbit = it.val;
                // printf("{:%llu, }:%llu\ni: %llu, j:%llu\n", mleftbit, mrightbit, i, j);
                if (0 < stackSize && stackSize <= indexMaxNesting) {
                    if (i == j) {
                        indexCollection[(stackSize - 1ULL)*n+i] = indexCollection[(stackSize - 1)*n+i] & ~(mrightbit - mleftbit);
                    } else {
                        indexCollection[(stackSize - 1ULL)*n+j] = indexCollection[(stackSize - 1)*n+j] & (mleftbit - 1);
                        indexCollection[(stackSize - 1ULL)*n+i] = indexCollection[(stackSize - 1)*n+i] & ~(mrightbit - 1);
                        int k;
                        // printf("j+1: %llu, i:%llu\n", j+1, i);
                        for (k = j + 1; k < i; k++) {
                            indexCollection[(stackSize-1ULL)*n+k] = 0;
                        }
                    }
                }
                mright = R(mright);
            }
        } while (mrightbit != 0);
    }
}

JNIEXPORT void JNICALL Java_com_hazelcast_query_misonparser_SIMDHelper_createCharacterIndexes(
    JNIEnv *env,
    jclass class,
    jstring text,
    jint maxDepth,
    jobject leveledColonIndex
) {
    prepareMasks();


    jsize stringLen = (*env)->GetStringLength(env, text);
    const u_int16_t *chars = (*env)->GetStringCritical(env, text, NULL);

    int indexArraySize = (((stringLen - 1)  >> 7) + 1) << 1;
    
    // printf("stirnglen: %d, bolu 7: %d, ias: %d\n", stringLen, stringLen >> 7, indexArraySize );

    uint64_t quoteIndex[indexArraySize];
    uint64_t lBraceIndex[indexArraySize];
    uint64_t rBraceIndex[indexArraySize];
    uint64_t backslashIndex[indexArraySize];
    uint64_t colonIndex[indexArraySize];


    fillIndex(chars, colonMask, indexArraySize, colonIndex);
    fillIndex(chars, leftBraceMask, indexArraySize, lBraceIndex);
    fillIndex(chars, rightBraceMask, indexArraySize, rBraceIndex);
    fillIndex(chars, quoteMask, indexArraySize, quoteIndex);
    fillIndex(chars, backslashMask, indexArraySize, backslashIndex);

    // printLongArrayLE(lBraceIndex, indexArraySize);
    // printf("\n");
    // printf("rb array ");
    // printLongArrayLE(rBraceIndex, indexArraySize);
    // printf("\n");

    (*env)->ReleaseStringCritical(env, text, chars);

    uint64_t *indexCollection = (*env)->GetDirectBufferAddress(env, leveledColonIndex);
    memset(indexCollection, 0, 8 * maxDepth * indexArraySize);
    buildLeveledColonMap(indexCollection, maxDepth, indexArraySize, colonIndex, lBraceIndex, rBraceIndex);

    // for (int i = 0; i < maxDepth; i++) {
    //     printf("level %d\n", i);
    //     printLongArrayLE(indexCollection+ i * indexArraySize, indexArraySize);
    //     printf("\n");
    // }


}