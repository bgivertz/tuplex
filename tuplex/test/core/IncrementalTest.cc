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
#include <context.h>
#include "TestUtils.h"

class IncrementalTest : public PyTest {};

TEST_F(IncrementalTest, Debug) {
    using namespace tuplex;
    using namespace std;

    auto opts = testOptions();
    opts.set("tuplex.optimizer.incrementalResolution", "true");
    opts.set("tuplex.optimizer.mergeExceptionsInOrder", "true");
    Context c(opts);

    auto numRows = 1000;
    auto amountExps = 0.1;
    std::vector<Row> inputRows;
    inputRows.reserve(numRows);

    for (int i = 0; i < numRows; ++i) {
        if (i % (int) (1 / amountExps) == 0) {
            inputRows.push_back(Row(0));
        } else {
            inputRows.push_back(Row(i));
        }
    }

    auto &ds_cached = c.parallelize(inputRows).cache();

    c.getIncrementalCache()->clear();

    auto res1 = ds_cached.map(UDF("lambda x: 1 / x if x == 0 else x")).collectAsVector();
    std::vector<Row> expectedRes1;
    copy_if(inputRows.begin(), inputRows.end(), std::back_inserter(expectedRes1), [](Row r){ return r.getInt(0) != 0; });
    ASSERT_EQ(res1.size(), expectedRes1.size());
    for (int i = 0; i < expectedRes1.size(); ++i) {
        EXPECT_EQ(res1[i].toPythonString(), expectedRes1[i].toPythonString());
    }

    auto res2 = ds_cached.map(UDF("lambda x: 1 / x if x == 0 else x")).resolve(ExceptionCode::ZERODIVISIONERROR, UDF("lambda x: -1")).collectAsVector();
    std::vector<Row> expectedRes2;
    transform(inputRows.begin(), inputRows.end(), std::back_inserter(expectedRes2), [](Row r){ return r.getInt(0) == 0 ? Row(-1) : r; });
    ASSERT_EQ(res2.size(), expectedRes2.size());
    for (int i = 0; i < expectedRes2.size(); ++i) {
        EXPECT_EQ(res2[i].toPythonString(), expectedRes2[i].toPythonString());
    }
}