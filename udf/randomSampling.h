#ifndef RANDOMSAMPLING_H
#define RANDOMSAMPLING_H

#include <impala_udf/udf.h>

using namespace impala_udf;

BooleanVal randomSamplingUDF(FunctionContext* context, const DoubleVal& arg1, const IntVal &arg2);
BooleanVal randomSamplingUDF(FunctionContext* context, const DoubleVal& arg1, const DoubleVal& arg2);
BooleanVal randomSamplingUDF(FunctionContext* context, const DoubleVal& arg1, const StringVal& arg2);
#endif // RANDOMSAMPLING_H
