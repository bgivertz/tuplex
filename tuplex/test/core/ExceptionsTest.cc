//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/1/2021                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <gtest/gtest.h>
#include <Context.h>
#include <physical/TuplexSourceTaskBuilder.h>
#include <physical/ExceptionSourceTaskBuilder.h>
#include "TestUtils.h"

class ExceptionsTest : public PyTest {};

//void printRows(std::vector<tuplex::Row> rows) {
//    printf("[");
//    for (int i = 0; i < rows.size() - 1; ++i) {
//        printf("%s, ", rows[i].toPythonString().c_str());
//    }
//    printf("%s]\n", rows[rows.size() - 1].toPythonString().c_str());
//}

TEST_F(ExceptionsTest, Playground) {
    using namespace tuplex;

    Context c(microTestOptions());

    auto ds = c.parallelize({
        Row(1),
        Row(2),
        Row(0),
        Row("E1"),
        Row(0),
        Row("E2"),
        Row(4),
        Row(5)
    });

    auto res1 = ds.map(UDF("lambda x: 1 // x if x == 0 else x")).collectAsVector();
    printRows(res1);

    auto res2 = ds.map(UDF("lambda x: 1 // x if x == 0 else x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1"))
            .collectAsVector(std::cout, true);
    printRows(res2);
}

TEST_F(ExceptionsTest, Timing) {
    using namespace tuplex;

    Context c(testOptions());

    std::vector<Row> data;
    for (int i = 0; i < 1000000; ++i) {
        if (i % 100 == 0) {
            data.push_back(Row(0));
        } else {
            data.push_back(Row(i));
        }
    }

    Timer timer;
    auto ds1 = c.parallelize(data)
            .map(UDF("lambda x: 1 / x"));

    auto res1 = ds1.collectAsVector();
    auto t1 = timer.time();
    timer.reset();

    auto ds2 = c.parallelize(data)
            .map(UDF("lambda x: 1 / x"))
            .resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1.0"));

    auto res2 = ds2.collectAsVector(std::cout, true);
    auto t2 = timer.time();

    printf("first one: %s\n", std::to_string(t1).c_str());
    printf("second one: %s\n", std::to_string(t2).c_str());
}


void testExceptionCallback(const int64_t ecCode, const int64_t opID, const int64_t row,
                           const uint8_t *buf, const size_t bufSize) {

}

void testWriter(uint8_t** outBuf, uint8_t* buf, int64_t bufSize) {
//    *outBuf = (uint8_t *) malloc(bufSize);
//    memcpy(*outBuf, buf, bufSize);
}

TEST_F(ExceptionsTest, Codegen) {
    using namespace std;
    using namespace tuplex;

    // Create environment
    auto env = make_shared<codegen::LLVMEnvironment>();

    // Create pipeline using pipeline builder
    auto normalCaseRowType = python::Type::propagateToTupleType(python::Type::I64);
    auto pip = make_shared<codegen::PipelineBuilder>(env, normalCaseRowType);

    // Pipeline Def
    auto udf = UDF("lambda x: x != 0");
    auto inputSchema = Schema(Schema::MemoryLayout::ROW, normalCaseRowType);
    udf.hintInputSchema(inputSchema);

    pip->filterOperation(0, udf, 0.5, false, false);
    pip->buildWithTuplexWriter("execRow_writeback", 1);

    auto tb = make_shared<codegen::ExceptionSourceTaskBuilder>(env, normalCaseRowType, "stage0");
    tb->setExceptionHandler("testExceptionCallback");
    tb->setPipeline(pip);
    tb->build();

    runtime::init(ContextOptions::defaults().RUNTIME_LIBRARY().toPath());

    auto ir = env->getIR();
    auto compiler = std::make_shared<JITCompiler>();
    compiler->registerSymbol("execRow_writeback", testWriter);
    compiler->registerSymbol("testExceptionCallback", testExceptionCallback);
    compiler->compile(ir);

    auto functor = (codegen::read_block_exp_f) compiler->getAddrOfSymbol("stage0");

    uint8_t *userData;

//    // [E1, 1, E2, 2, E3, 0, E4, 0, E5, 3, E6]
//    // [E1, 1, E2, 2, E3, E4, E5, 3, E6]
//
//    int64_t inPtr[6] = {5, 1, 2, 0, 0, 3};
//    int64_t inPtrSize = 5;
//
//    int64_t expPtr1[15] = {0, -1, -1, 8, -1,
//                           2, -1, -1, 8, -1,
//                           4, -1, -1, 8, -1};
//    int64_t expPtr2[15] = {6, -1, -1, 8, -1,
//                           8, -1, -1, 8, -1,
//                           10, -1, -1, 8, -1};
//    uint8_t *expPtrs[2] = {(uint8_t *) expPtr1, (uint8_t *) expPtr2};
//    int64_t expPtrSizes[2] = {3, 3};
//    int64_t numExps = 6;

    int64_t inPtr[3] = {2, 1, 0};
    int64_t inPtrSize = 2;

    int64_t expPtr1[5] = {1, -1, -1, 8, -1};
    uint8_t *expPtrs[1] = {(uint8_t *) expPtr1};
    int64_t expPtrSizes[1] = {1};
    int64_t numExps = 1;

    int64_t outNormalRowCount = 0;
    int64_t outBadRowCount = 0;
    bool ignoreLastRow = false;

    functor(userData, (uint8_t *) inPtr, inPtrSize, expPtrs, expPtrSizes, numExps, &outNormalRowCount, &outBadRowCount,
            ignoreLastRow);

    ASSERT_EQ(expPtr1[0], 1);
//    ASSERT_EQ(expPtr1[5], 2);
//    ASSERT_EQ(expPtr1[10], 4);
//
//    ASSERT_EQ(expPtr2[0], 5);
//    ASSERT_EQ(expPtr2[5], 6);
//    ASSERT_EQ(expPtr2[10], 8);
}

void process(uint8_t *inPtr, uint8_t **expPtrs, int64_t *expPtrSizes, int64_t numExps, int64_t *outNormalRowCount);

TEST_F(ExceptionsTest, Debug) {
    int64_t inPtr[5] = {4, 1, 2, 4, 5};

    int64_t expPtr1[2] = {0, 3};
    int64_t expPtr2[1] = {6};
    uint8_t *expPtrs[2] = {(uint8_t *) expPtr1, (uint8_t *) expPtr2};

    int64_t expPtrSizes[2] = {2, 1};

    int64_t numExps = 3;

    int64_t outNormalRowCount = 0;

    process((uint8_t *) inPtr, expPtrs, expPtrSizes, numExps, &outNormalRowCount);
}

TEST_F(ExceptionsTest, Debug2) {
    int64_t inPtr[5] = {2, 0, 0};

    int64_t expPtr[1] = {0};
    uint8_t *expPtrs[1] = {(uint8_t *) expPtr};

    int64_t expPtrSizes[1] = {1};

    int64_t numExps = 1;

    int64_t outNormalRowCount = 0;

    process((uint8_t *) inPtr, expPtrs, expPtrSizes, numExps, &outNormalRowCount);
}

TEST_F(ExceptionsTest, Debug3) {
    int64_t inPtr[6] = {5, 1, 2, 0, 0, 3};

    int64_t expPtr1[3] = {0, 2, 4};
    int64_t expPtr2[3] = {6, 8, 10};
    uint8_t *expPtrs[2] = {(uint8_t *) expPtr1, (uint8_t *) expPtr2};
    int64_t expPtrSizes[2] = {3, 3};

    int64_t numExps = 6;
    int64_t outNormalRowCount = 0;

    process((uint8_t *) inPtr, expPtrs, expPtrSizes, numExps, &outNormalRowCount);

    ASSERT_EQ(expPtr1[0], 0);
    ASSERT_EQ(expPtr1[1], 2);
    ASSERT_EQ(expPtr1[2], 4);
    ASSERT_EQ(expPtr2[0], 5);
    ASSERT_EQ(expPtr2[1], 6);
    ASSERT_EQ(expPtr2[2], 8);
}

void process(uint8_t *inPtr, uint8_t **expPtrs, int64_t *expPtrSizes, int64_t numExps, int64_t *outNormalRowCount) {
    int64_t inSize = *((int64_t *) inPtr); inPtr += sizeof(int64_t);

    int64_t curExpIndVar = 0;
    uint8_t *curExpPtrVar = *expPtrs;
    int64_t curExpNumRowsVar = *expPtrSizes;
    int64_t curExpCurRowVar = 0;
    int64_t expAccVar = 0;
    int64_t expCurRowVar = 0;


    for (int i = 0; i < inSize; ++i) {
        int64_t curVal = *((int64_t *) inPtr); inPtr += sizeof(int64_t);
        bool filtered = true;
        if (curVal != 0) {
            filtered = false;
        }
        *outNormalRowCount += 1;

        if (filtered && expCurRowVar < numExps) {
            while (expCurRowVar < numExps && ((*outNormalRowCount - 1) + expCurRowVar) >= *((int64_t *) curExpPtrVar)) {
                    *((int64_t *) curExpPtrVar) -= expAccVar;
                    curExpPtrVar += sizeof(int64_t);
                    expCurRowVar += 1;
                    curExpCurRowVar += 1;

                    if (expCurRowVar < numExps && curExpCurRowVar >= curExpNumRowsVar) {
                        curExpCurRowVar = 0;
                        curExpIndVar = curExpIndVar + 1;
                        curExpPtrVar = expPtrs[curExpIndVar];
                        curExpNumRowsVar = expPtrSizes[curExpIndVar];
                    }
            }

            expAccVar += 1;
        }

    }

    while (expCurRowVar < numExps) {
        *((int64_t *) curExpPtrVar) -= expAccVar;
        curExpPtrVar += sizeof(int64_t);
        expCurRowVar += 1;
        curExpCurRowVar += 1;

        if (expCurRowVar < numExps && curExpCurRowVar >= curExpNumRowsVar) {
            curExpCurRowVar = 0;
            curExpIndVar = curExpIndVar + 1;
            curExpPtrVar = expPtrs[curExpIndVar];
            curExpNumRowsVar = expPtrSizes[curExpIndVar];
        }
    }
}