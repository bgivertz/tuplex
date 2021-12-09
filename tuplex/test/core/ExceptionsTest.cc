//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <Context.h>
#include <physical/TuplexSourceTaskBuilder.h>
#include <physical/ExceptionTuplexSourceTaskBuilder.h>
#include "TestUtils.h"

void serializeRows(uint8_t **buf, size_t *bufSize, const std::vector<tuplex::Row>& rows);
std::vector<tuplex::Row> deserializeRows(uint8_t *buf, size_t bufSize, const python::Type& rowType);
void testSerializeRows(std::vector<tuplex::Row> rows);

void serializeExceptions(uint8_t **buf, size_t *bufSize, const std::vector<std::tuple<size_t, tuplex::Row>>& rows);
std::vector<std::tuple<size_t, tuplex::Row>> deserializeExceptions(uint8_t *buf, size_t bufSize, const python::Type& rowType);
void testSerializeExceptions(std::vector<std::tuple<size_t, tuplex::Row>> exceptions);

typedef int64_t (*functor_t)(void *,const unsigned char *,long long int,long long int);
void testWriter(uint8_t** outBuf, uint8_t* buf, int64_t bufSize);
void process(functor_t functor, uint8_t *normalBuf, size_t normalBufSize, uint8_t *exceptionsBuf, size_t exceptionsBufSize, uint8_t **outBuf, size_t *outBufSize);
std::vector<tuplex::Row> mergeExceptions(std::vector<tuplex::Row> normalCaseOutput, std::vector<std::tuple<size_t, tuplex::Row>> exceptions);
void compareRows(std::vector<tuplex::Row> normalRows, std::vector<std::tuple<size_t, tuplex::Row>> exceptions, functor_t functor, std::vector<tuplex::Row> expectedOutput);

void testExceptionCallback(const int64_t ecCode, const int64_t opID, const int64_t row,
                                               const uint8_t *buf, const size_t bufSize) {

}

TEST(ExceptionsTest, IRCode) {
    using namespace std;
    using namespace tuplex;

    // Step 1. Get environemnt
    auto env = std::make_shared<codegen::LLVMEnvironment>();
//    cout << env->getIR() << endl;

    // Step 2. Create pipeline using pipeline builder
    auto normalCaseRowType = python::Type::propagateToTupleType(python::Type::I64);
    auto pip = std::make_shared<codegen::PipelineBuilder>(env, normalCaseRowType);
//
    // Pipeline Def
    auto udf = UDF("lambda x: x > 3");
    auto inputSchema = Schema(Schema::MemoryLayout::ROW, normalCaseRowType);
    udf.hintInputSchema(inputSchema);
//
    pip->filterOperation(0, udf, 0.5, false, false);
    pip->buildWithTuplexWriter("execRow_writeback", 1);
//
//    cout << "\n\n\nStep 2:\n";
//
//    // Step 3. Create block based reader using tuplex source
    auto tb = make_shared<codegen::ExceptionTuplexSourceTaskBuilder>(env, normalCaseRowType, "stage0");
    tb->setExceptionHandler("testExceptionCallback");
    tb->setPipeline(pip);
    tb->build();
//    // add more fields

//
//    // Step 4. load runtime and compile
    auto opts = microTestOptions();
    runtime::init(opts.RUNTIME_LIBRARY().toPath());
//
    auto ir = env->getIR();
    cout << ir << endl;
    auto compiler = std::make_shared<JITCompiler>();
    compiler->registerSymbol("execRow_writeback", testWriter);
    compiler->registerSymbol("testExceptionCallback", testExceptionCallback);
    compiler->compile(ir);

    auto functor = (codegen::read_block_exp_f)compiler->getAddrOfSymbol("stage0");

    uint8_t *userData;
//    std::vector<Row> data({Row(1), Row(2), Row(4), Row(5)});
//    std::vector<Row> data({Row(2)});
//    uint8_t *buf = nullptr;
//    size_t bufSize;
//    serializeRows(&buf, &bufSize, data);

    int64_t buf[5] = {4, 1, 2, 4, 5};
//    int64_t buf[2] = {1, 1, 2, 4, 5};


//    std::vector<std::tuple<size_t, Row>> exps({std::make_tuple(0,Row("E1")), std::make_tuple(3, Row("E2")), std::make_tuple(6, Row("E3"))});
//    uint8_t *expBuf = nullptr;
//    size_t expBufSize;
//    serializeExceptions(&expBuf, &expBufSize, exps);
    int64_t expBuf[10] = {3, 0, sizeof(int64_t), -1,  3,  sizeof(int64_t), -1, 6, sizeof(int64_t), -1};
//    int64_t expBuf[4] = {1, 0, -1, };

    int64_t outNormalRowCount = 0;
    int64_t outBadRowCount = 0;
    bool ignoreLastRow = false;

    functor(userData, (uint8_t *) buf, 4, (uint8_t *) expBuf, &outNormalRowCount, &outBadRowCount, ignoreLastRow);

//    auto newExps = deserializeExceptions(expBuf, expBufSize, python::Type::propagateToTupleType(python::Type::STRING));

    auto newVal = 123;
    // Step 5. invoke functor over row


}

TEST(ExceptionsTest, RowSerialization) {
    using namespace tuplex;

    testSerializeRows({Row(1), Row(2), Row(3)});
    testSerializeRows({Row("a"), Row("b"), Row("c")});
    testSerializeRows({Row(true), Row(false), Row(true)});
    testSerializeRows({Row(1, "a", true), Row(2, "b", false), Row(3, "c", true)});
    testSerializeRows({Row(1, Tuple(1, "a")), Row(2, Tuple(2, "b")), Row(3, Tuple(3, "c"))});
}

TEST(ExceptionsTest, ExceptionSerialization) {
    using namespace tuplex;

    testSerializeExceptions({std::make_tuple(0, Row(1)), std::make_tuple(1, Row(2)), std::make_tuple(2, Row(3))});
    testSerializeExceptions({std::make_tuple(0, Row("a")), std::make_tuple(1, Row("b")), std::make_tuple(3, Row("c"))});
    testSerializeExceptions({std::make_tuple(0, Row(true)), std::make_tuple(1, Row(false)), std::make_tuple(2, Row(true))});
    testSerializeExceptions({std::make_tuple(0, Row(1, "a", true)), std::make_tuple(2, Row(2, "b", false)), std::make_tuple(3, Row(3, "c", true))});
    testSerializeExceptions({std::make_tuple(0, Row(1, Tuple(1, "a"))), std::make_tuple(1, Row(2, Tuple(2, "b"))), std::make_tuple(3, Row(3, Tuple(3, "c")))});
}

TEST(ExceptionsTest, Merge1) {
    using namespace tuplex;
    std::vector<Row> normalCaseRows({Row(1), Row(2), Row(4), Row(5)});
    std::vector<std::tuple<size_t, Row>> exceptions({std::make_tuple(0, "E1"), std::make_tuple(3, "E2"), std::make_tuple(6, "E3")});
    std::vector<Row> expectedOutput({Row("E1"), Row("E2"), Row(4), Row(5), Row("E3")});

    // Pipeline Init
    auto normalCaseRowType = normalCaseRows.at(0).getRowType();
    auto opts = microTestOptions();
    runtime::init(opts.RUNTIME_LIBRARY().toPath());
    auto env = std::make_shared<codegen::LLVMEnvironment>();
    auto pip = std::make_shared<codegen::PipelineBuilder>(env, normalCaseRowType);

    // Pipeline Def
    auto udf = UDF("lambda x: x > 3");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, normalCaseRowType));

    pip->filterOperation(0, udf, 0.5, false, false);
    pip->buildWithTuplexWriter("execRow_writeback", 1);

    // Functor Init
    auto llvmFunc = codegen::createSingleProcessRowWrapper(*pip.get(), "execRow");
    std::string funName = llvmFunc->getName();
    auto ir = env->getIR();
    auto compiler = std::make_shared<JITCompiler>();
    compiler->registerSymbol("execRow_writeback", testWriter);
    compiler->compile(ir);
    auto functor = (codegen::process_row_f)compiler->getAddrOfSymbol(funName);

    compareRows(normalCaseRows, exceptions, functor, expectedOutput);
}

TEST(ExceptionsTest, Merge2) {
    using namespace tuplex;
    std::vector<Row> normalCaseRows({Row(1), Row(2), Row(4), Row(5)});
    std::vector<std::tuple<size_t, Row>> exceptions({std::make_tuple(0, "E1"),  std::make_tuple(1, "E2"), std::make_tuple(10, "E3"), std::make_tuple(12, "E4")});
    std::vector<Row> expectedOutput({Row("E1"), Row("E2"), Row("E3"), Row("E4")});

    // Pipeline Init
    auto normalCaseRowType = normalCaseRows.at(0).getRowType();
    auto opts = microTestOptions();
    runtime::init(opts.RUNTIME_LIBRARY().toPath());
    auto env = std::make_shared<codegen::LLVMEnvironment>();
    auto pip = std::make_shared<codegen::PipelineBuilder>(env, normalCaseRowType);

    // Pipeline Def
    auto udf = UDF("lambda x: x > 5");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, normalCaseRowType));

    pip->filterOperation(0, udf, 0.5, false, false);
    pip->buildWithTuplexWriter("execRow_writeback", 1);

    // Functor Init
    auto llvmFunc = codegen::createSingleProcessRowWrapper(*pip.get(), "execRow");
    std::string funName = llvmFunc->getName();
    auto ir = env->getIR();
    auto compiler = std::make_shared<JITCompiler>();
    compiler->registerSymbol("execRow_writeback", testWriter);
    compiler->compile(ir);
    auto functor = (codegen::process_row_f)compiler->getAddrOfSymbol(funName);

    compareRows(normalCaseRows, exceptions, functor, expectedOutput);
}

TEST(ExceptionsTest, Merge3) {
    using namespace tuplex;
    std::vector<Row> normalCaseRows({Row(1), Row(2), Row(3), Row(4)});
    std::vector<std::tuple<size_t, Row>> exceptions;
    std::vector<Row> expectedOutput({Row(1), Row(2), Row(3), Row(4)});

    // Pipeline Init
    auto normalCaseRowType = normalCaseRows.at(0).getRowType();
    auto opts = microTestOptions();
    runtime::init(opts.RUNTIME_LIBRARY().toPath());
    auto env = std::make_shared<codegen::LLVMEnvironment>();
    auto pip = std::make_shared<codegen::PipelineBuilder>(env, normalCaseRowType);

    // Pipeline Def
    auto udf = UDF("lambda x: x < 5");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, normalCaseRowType));

    pip->filterOperation(0, udf, 0.5, false, false);
    pip->buildWithTuplexWriter("execRow_writeback", 1);

    // Functor Init
    auto llvmFunc = codegen::createSingleProcessRowWrapper(*pip.get(), "execRow");
    std::string funName = llvmFunc->getName();
    auto ir = env->getIR();
    auto compiler = std::make_shared<JITCompiler>();
    compiler->registerSymbol("execRow_writeback", testWriter);
    compiler->compile(ir);
    auto functor = (codegen::process_row_f)compiler->getAddrOfSymbol(funName);

    compareRows(normalCaseRows, exceptions, functor, expectedOutput);
}

TEST(ExceptionsTest, Merge4) {
    using namespace tuplex;
    std::vector<Row> normalCaseRows;
    for (int i = 0; i < 100; ++i) {
        normalCaseRows.push_back(Row(i));
    }
    std::vector<std::tuple<size_t, Row>> exceptions;
    for (int i = 0; i < 200; i+=2) {
        exceptions.push_back(std::make_tuple(i, Row("E")));
    }
    std::vector<Row> expectedOutput;
    for (int i = 0; i < 200; ++i) {
        if (i % 2 == 0) {
            expectedOutput.push_back(Row("E"));
        } else {
            expectedOutput.push_back(Row(i / 2));
        }
    }

    // Pipeline Init
    auto normalCaseRowType = normalCaseRows.at(0).getRowType();
    auto opts = microTestOptions();
    runtime::init(opts.RUNTIME_LIBRARY().toPath());
    auto env = std::make_shared<codegen::LLVMEnvironment>();
    auto pip = std::make_shared<codegen::PipelineBuilder>(env, normalCaseRowType);

    // Pipeline Def
    auto udf = UDF("lambda x: x");
    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, normalCaseRowType));

    pip->mapOperation(0, udf, 0.5, false, false);
    pip->buildWithTuplexWriter("execRow_writeback", 1);

    // Functor Init
    auto llvmFunc = codegen::createSingleProcessRowWrapper(*pip.get(), "execRow");
    std::string funName = llvmFunc->getName();
    auto ir = env->getIR();
    auto compiler = std::make_shared<JITCompiler>();
    compiler->registerSymbol("execRow_writeback", testWriter);
    compiler->compile(ir);
    auto functor = (codegen::process_row_f)compiler->getAddrOfSymbol(funName);

    compareRows(normalCaseRows, exceptions, functor, expectedOutput);
}

void serializeRows(uint8_t **buf, size_t *bufSize, const std::vector<tuplex::Row>& rows) {
    *bufSize = 0;
    for (auto row : rows) {
        *bufSize += sizeof(size_t) + row.serializedLength();
    }

    *buf = (uint8_t *) malloc(*bufSize);

    auto ptr = *buf;
    auto bytesWritten = 0;
    for (auto row : rows) {
        auto rowSize = row.serializedLength();
        *((size_t *) ptr) = rowSize;
        ptr += sizeof(size_t);
        row.serializeToMemory(ptr, *bufSize - bytesWritten);
        ptr += rowSize;
        bytesWritten += sizeof(size_t) + rowSize;
    }
}

std::vector<tuplex::Row> deserializeRows(uint8_t *buf, size_t bufSize, const python::Type& rowType) {
    using namespace tuplex;

    std::vector<Row> result;

    auto ptr = buf;
    while (ptr < buf + bufSize) {
        auto rowSize = *((size_t*) ptr);
        ptr += sizeof(size_t);
        Row row = Row::fromMemory(Schema(Schema::MemoryLayout::ROW, rowType), ptr, buf + bufSize - ptr);
        ptr += rowSize;

        result.push_back(row);
    }

    free(buf);
    return result;
}

void testSerializeRows(std::vector<tuplex::Row> rows) {
    using namespace tuplex;

    uint8_t *buf;
    size_t bufSize;
    serializeRows(&buf, &bufSize, rows);
    auto result = deserializeRows(buf, bufSize, rows.at(0).getRowType());

    ASSERT_EQ(rows.size(), result.size());
    for (int i = 0; i < rows.size(); ++i) {
        EXPECT_EQ(rows.at(i).toPythonString(), result.at(i).toPythonString());
    }
}

void serializeExceptions(uint8_t **buf, size_t *bufSize, const std::vector<std::tuple<size_t, tuplex::Row>>& rows) {
    *bufSize = 0;
    for (auto elt : rows) {
        auto row = std::get<1>(elt);
        *bufSize += sizeof(size_t) * 2 + row.serializedLength();
    }

    *buf = (uint8_t *) malloc(*bufSize);

    auto ptr = *buf;
    auto bytesWritten = 0;
    for (auto elt : rows) {
        auto rowInd = std::get<0>(elt);
        auto row = std::get<1>(elt);

        *((size_t *) ptr) = rowInd;
        ptr += sizeof(size_t);

        auto rowSize = row.serializedLength();
        *((size_t *) ptr) = rowSize;
        ptr += sizeof(size_t);

        row.serializeToMemory(ptr, *bufSize - bytesWritten);
        ptr += rowSize;

        bytesWritten += 2 * sizeof(size_t) + rowSize;
    }
}

std::vector<std::tuple<size_t, tuplex::Row>> deserializeExceptions(uint8_t *buf, size_t bufSize, const python::Type& rowType) {
    using namespace tuplex;

    std::vector<std::tuple<size_t, Row>> result;

    auto ptr = buf;
    while (ptr < buf + bufSize) {
        auto ind = *((size_t*) ptr);
        ptr += sizeof(size_t);
        auto rowSize = *((size_t*) ptr);
        ptr += sizeof(size_t);
        Row row = Row::fromMemory(Schema(Schema::MemoryLayout::ROW, rowType), ptr, buf + bufSize - ptr);
        ptr += rowSize;

        result.push_back(std::make_tuple(ind, row));
    }

    free(buf);
    return result;
}

void testSerializeExceptions(std::vector<std::tuple<size_t, tuplex::Row>> exceptions) {
    using namespace tuplex;

    uint8_t *buf;
    size_t bufSize;
    serializeExceptions(&buf, &bufSize, exceptions);
    auto rowType = std::get<1>(exceptions.at(0)).getRowType();
    auto result = deserializeExceptions(buf, bufSize, rowType);

    ASSERT_EQ(exceptions.size(), result.size());
    for (int i = 0; i < exceptions.size(); ++i) {
        auto expectedInd = std::get<0>(exceptions.at(i));
        auto expectedRow = std::get<1>(exceptions.at(i));
        auto resultInd = std::get<0>(result.at(i));
        auto resultRow = std::get<1>(result.at(i));

        EXPECT_EQ(expectedInd, resultInd);
        EXPECT_EQ(expectedRow.toPythonString(), resultRow.toPythonString());
    }
}

void testWriter(uint8_t** outBuf, uint8_t* buf, int64_t bufSize) {
//    *outBuf = (uint8_t *) malloc(bufSize);
//    memcpy(*outBuf, buf, bufSize);
}

void process(functor_t functor, uint8_t *normalBuf, size_t normalBufSize, uint8_t *exceptionsBuf, size_t exceptionsBufSize, uint8_t **outBuf, size_t *outBufSize) {
    size_t normalRowInd = 0;
    size_t exceptionsAcc = 0;
    uint8_t *exceptionsPtr = exceptionsBuf;

    uint8_t *ptr = normalBuf;


    int64_t numExceptions = 0;
    *outBufSize = 0;
    *outBuf = (uint8_t *) malloc(normalBufSize);
    uint8_t *outPtr = *outBuf;
    while (ptr < normalBuf + normalBufSize) {
        size_t rowSize = *((size_t *) ptr);
        ptr += sizeof(size_t);
        uint8_t *row = (uint8_t *) malloc(rowSize);
        memcpy(row, ptr, rowSize);
        ptr += rowSize;

        uint8_t *tempOutBuf = nullptr;
        size_t outBytes = functor(&tempOutBuf, row, 0, 0);

        if (!tempOutBuf) {
            size_t exceptionsInd = *((size_t *) exceptionsPtr);

            while (normalRowInd + numExceptions >= exceptionsInd) {
                *((size_t *) exceptionsPtr) -= exceptionsAcc;
                exceptionsPtr += sizeof(size_t);
                exceptionsPtr += *((size_t *) exceptionsPtr) + sizeof(size_t);
                exceptionsInd = *((size_t *) exceptionsPtr);
                numExceptions += 1;
            }

            exceptionsAcc += 1;

        } else {
            *((size_t *) outPtr) = outBytes;
            outPtr += sizeof(size_t);
            memcpy(outPtr, tempOutBuf, outBytes);
            free(tempOutBuf);

            outPtr += outBytes;
            *outBufSize += outBytes + sizeof(size_t);
        }
        normalRowInd++;
    }

    while (exceptionsPtr < exceptionsBuf + exceptionsBufSize) {
        *((size_t *) exceptionsPtr) -= exceptionsAcc;

        exceptionsPtr += sizeof(size_t);
        exceptionsPtr += *((size_t *) exceptionsPtr) + sizeof(size_t);
    }

    *outBuf = (uint8_t *) realloc(*outBuf, *outBufSize);
}

std::vector<tuplex::Row> mergeExceptions(std::vector<tuplex::Row> normalCaseOutput, std::vector<std::tuple<size_t, tuplex::Row>> exceptions) {
    using namespace tuplex;
    std::vector<Row> result;

    int i = 0;
    int j = 0;
    while (i < exceptions.size()) {
        auto curExceptionInd = std::get<0>(exceptions.at(i));
        auto curException = std::get<1>(exceptions.at(i));

        while (j < normalCaseOutput.size() && result.size() < curExceptionInd) {
            result.push_back(normalCaseOutput.at(j));
            j += 1;
        }

        result.push_back(curException);
        i += 1;
    }

    while (j < normalCaseOutput.size()) {
        result.push_back(normalCaseOutput.at(j));
        j += 1;
    }

    return result;
}

void compareRows(std::vector<tuplex::Row> normalRows, std::vector<std::tuple<size_t, tuplex::Row>> exceptions, functor_t functor, std::vector<tuplex::Row> expectedOutput) {
    using namespace tuplex;
    using namespace tuplex;

    uint8_t *normalBuf;
    size_t normalBufSize;
    serializeRows(&normalBuf, &normalBufSize, normalRows);

    uint8_t *exceptionsBuf;
    size_t exceptionsBufSize;
    serializeExceptions(&exceptionsBuf, &exceptionsBufSize, exceptions);

    uint8_t *outBuf;
    size_t outBufSize;
    process(functor, normalBuf, normalBufSize, exceptionsBuf, exceptionsBufSize, &outBuf, &outBufSize);

    std::vector<std::tuple<size_t, Row>> exceptionsOutput;
    if (exceptions.size() > 0) {
        exceptionsOutput = deserializeExceptions(exceptionsBuf, exceptionsBufSize, std::get<1>(exceptions.at(0)).getRowType());
    }

    std::vector<Row> normalOutput;
    if (normalRows.size() > 0) {
        normalOutput = deserializeRows(outBuf, outBufSize, normalRows.at(0).getRowType());
    }


    auto testOutput = mergeExceptions(normalOutput, exceptionsOutput);

    ASSERT_EQ(testOutput.size(), expectedOutput.size());
    for (int i = 0; i < expectedOutput.size(); ++i) {
        EXPECT_EQ(testOutput.at(i).toPythonString(), expectedOutput.at(i).toPythonString());
    }
}

//
//
//
//
//// Process functions
//std::vector<tuplex::Row> mergeExceptions(std::vector<tuplex::Row> normalCaseOutput, std::vector<std::tuple<size_t, tuplex::Row>> exceptions, size_t *removedRows, size_t numRemovedRows);
//typedef int64_t (*functor_t)(void *,const unsigned char *,long long int,long long int);
//void process(functor_t functor,
//             uint8_t *normalBuf, uint8_t normalBufSize,
//             uint8_t *exceptionsBuf, uint8_t exceptionsBufSize,
//             uint8_t *outBuf, size_t *bytesWritten);
//void execRow_writer2(uint8_t** outBuf, uint8_t* buf, int64_t bufSize);
//
//// Serialization functions
//void serializeRows(uint8_t **buf, size_t *bufSize, const std::vector<tuplex::Row>& rows);
//std::vector<tuplex::Row> deserializeRows(uint8_t *buf, size_t bufSize, const python::Type& rowType);
//void serializeExceptions(uint8_t **buf, size_t *bufSize, const std::vector<std::tuple<size_t, tuplex::Row>>& rows);
//void testSerialize(std::vector<tuplex::Row> rows);
//
//TEST(ExceptionsTest, Filter) {
//    using namespace tuplex;
//
//    std::vector<Row> normalCaseRows({Row(1), Row(2), Row(4), Row(5)});
//    auto normalCaseRowType = normalCaseRows.at(0).getRowType();
//
//    std::vector<std::tuple<size_t, Row>> exceptions({std::make_tuple(0, Row("EXP0")), std::make_tuple(3, Row("EXP3")), std::make_tuple(6, Row("EXP6"))});
//
//    std::vector<Row> expectedResult({Row("EXP0"), Row("EXP3"), Row(4), Row(5), Row("EXP6")});
//
//    // Pipeline Init
//    auto opts = microTestOptions();
//    runtime::init(opts.RUNTIME_LIBRARY().toPath());
//    auto env = std::make_shared<codegen::LLVMEnvironment>();
//    auto pip = std::make_shared<codegen::PipelineBuilder>(env, normalCaseRowType);
//
//    // Pipeline Def
//    auto udf = UDF("lambda x: x > 3");
//    udf.hintInputSchema(Schema(Schema::MemoryLayout::ROW, normalCaseRowType));
//
//    pip->filterOperation(0, udf, 0.5, false, false);
//    pip->buildWithTuplexWriter("execRow_writeback", 1);
//
//    // Functor Init
//    auto llvmFunc = codegen::createSingleProcessRowWrapper(*pip.get(), "execRow");
//    std::string funName = llvmFunc->getName();
//    auto ir = env->getIR();
//    auto compiler = std::make_shared<JITCompiler>();
//    compiler->registerSymbol("execRow_writeback", execRow_writer2);
//    compiler->compile(ir);
//    auto functor = (codegen::process_row_f)compiler->getAddrOfSymbol(funName);
//
//    uint8_t *buf;
//    size_t bufSize;
//    serializeRows(&buf, &bufSize, normalCaseRows);
//
//    size_t *removedRows = nullptr;
//    size_t numRemovedRows = 0;
//
//    auto outBuf = (uint8_t *) malloc(bufSize);
//
//    size_t bytesWritten = 0;
//
//    uint8_t *exceptionsBuf;
//    size_t exceptionsBufSize;
//
//    process(functor, buf, bufSize, &removedRows, &numRemovedRows, outBuf, &bytesWritten);
//
//    runtime::releaseRunTimeMemory();
//
//    auto normalCaseOutput = deserializeRows(outBuf, bytesWritten, normalCaseRowType);
//
//    auto results = mergeExceptions(normalCaseOutput, exceptions, removedRows, numRemovedRows);
//
//    ASSERT_EQ(results.size(), expectedResult.size());
//    for (int i = 0; i < results.size(); ++i) {
//        EXPECT_EQ(expectedResult.at(i).toPythonString(), results.at(i).toPythonString());
//    }
//}
//
//void testMerge(std::vector<tuplex::Row> testOutput, std::vector<tuplex::Row> expectedOutput) {
//    ASSERT_EQ(testOutput.size(), expectedOutput.size());
//    for (int i = 0; i < expectedOutput.size(); ++i) {
//        EXPECT_EQ(testOutput.at(i).toPythonString(), expectedOutput.at(i).toPythonString());
//    }
//}
//
//
//

//
//void execRow_writer2(uint8_t** outBuf, uint8_t* buf, int64_t bufSize) {
//    // copy runtime buffer to test buffer
//    assert(outBuf);
//    *outBuf = (uint8_t*)malloc(bufSize);
//    memcpy(*outBuf, buf, bufSize);
//}
//
//
////void process(functor_t functor, uint8_t *buf, uint8_t bufSize, size_t **removedRows, size_t *numRemovedRows, uint8_t *outBuf, size_t *numNormalCaseOutput, size_t *totalBytesWritten) {
////    size_t normalRowInd = 0;
////    uint8_t *ptr = buf;
////
////    size_t removedRowsCapacity = 10;
////    *removedRows = (size_t *) malloc(sizeof(size_t) * removedRowsCapacity);
////
////    while (ptr < buf + bufSize) {
////        size_t rowSize = *((size_t *) ptr);
////        ptr += sizeof(size_t);
////        uint8_t *row = (uint8_t*) malloc(rowSize);
////        memcpy(row, ptr, rowSize);
////        ptr += rowSize;
////
////        uint8_t *tempOutBuf = nullptr;
////
////        auto bytesWritten = functor(&tempOutBuf, row, 0, 0);
////
////        if (!tempOutBuf) {
////            if (*numRemovedRows == removedRowsCapacity) {
////                removedRowsCapacity *= 2;
////                size_t *newRemovedRows = (size_t *) malloc(sizeof(size_t) * removedRowsCapacity);
////                memcpy(newRemovedRows, *removedRows, sizeof(size_t) * *numRemovedRows);
////                free(*removedRows);
////                *removedRows = newRemovedRows;
////            }
////
////            (*removedRows)[*numRemovedRows] = normalRowInd;
////            *numRemovedRows += 1;
////        } else {
////            *((size_t *)outBuf) = bytesWritten;
////            outBuf += sizeof(size_t);
////            memcpy(outBuf, tempOutBuf, bytesWritten);
////            outBuf += bytesWritten;
////            *numNormalCaseOutput += 1;
////            free(tempOutBuf);
////
////            *totalBytesWritten += bytesWritten + sizeof(size_t);
////        }
////
////        free(row);
////
////        normalRowInd += 1;
////    }
////}
//
//std::vector<tuplex::Row> mergeExceptions(std::vector<tuplex::Row> normalCaseOutput, std::vector<std::tuple<size_t, tuplex::Row>> exceptions, size_t *removedRows, size_t numRemovedRows) {
//    using namespace tuplex;
//    std::vector<Row> results;
//
//    int i = 0;
//    int j = 0;
//    int k = 0;
//    while (i < exceptions.size()) {
//        auto exceptInd = std::get<0>(exceptions.at(i));
//        auto exceptVal = std::get<1>(exceptions.at(i));
//
//        while ((k < numRemovedRows) && (removedRows[k] < exceptInd)) {
//            k += 1;
//        }
//
//        while ((j < normalCaseOutput.size()) && (results.size() < (exceptInd - k))) {
//            results.push_back(normalCaseOutput.at(j));
//            j += 1;
//        }
//
//        results.push_back(exceptVal);
//        i += 1;
//    }
//
//    while (j < normalCaseOutput.size()) {
//        results.push_back(normalCaseOutput.at(j));
//        j += 1;
//    }
//
//    return results;
//}
//

