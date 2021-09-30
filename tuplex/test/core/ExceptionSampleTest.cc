//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 9/30/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <Context.h>
#include "TestUtils.h"

using namespace tuplex;

class Exceptions : public PyTest {};

TEST_F(Exceptions, Demo) {
    Context co(microTestOptions());
    auto ds = co.parallelize({Row(1, 1), Row(1, 0), Row(2, 0), Row(2, 1)})
        .map(UDF("lambda a, b : a / b"))
        .collectAsVector();
    EXPECT_EQ(ds.size(), 2);
}