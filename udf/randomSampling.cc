#include "randomSampling.h"
#include <iostream>
#include <stdlib.h>

using namespace std;
BooleanVal randomSamplingUDF(FunctionContext *context, const DoubleVal &arg1, const DoubleVal &arg2)
{
    int randVal = rand() % 100000;
    return randVal < arg1.val * 100000;
}
BooleanVal randomSamplingUDF(FunctionContext *context, const DoubleVal &arg1, const IntVal &arg2)
{
    int randVal = rand() % 100000;
    return randVal < arg1.val * 100000;
}
BooleanVal randomSamplingUDF(FunctionContext *context, const DoubleVal &arg1, const StringVal &arg2)
{
    int randVal = rand() % 100000;
    return randVal < arg1.val * 100000;
}
