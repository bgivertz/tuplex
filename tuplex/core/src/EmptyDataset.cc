//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Leonhard Spiegelberg first on 1/1/2021                                                                 //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <EmptyDataset.h>

namespace tuplex {
    std::shared_ptr<ResultSet> EmptyDataset::take(int64_t numElements, std::ostream &os, bool incremental) {
        return std::make_shared<ResultSet>();
    }

    std::vector<Row> EmptyDataset::takeAsVector(int64_t numElements, std::ostream &os, bool incremental) {
        return std::vector<Row>{};
    }

    std::shared_ptr<ResultSet> EmptyDataset::collect(std::ostream &os, bool incremental) {
        return take(0, os, incremental);
    }

    std::vector<Row> EmptyDataset::collectAsVector(std::ostream &os, bool incremental) {
        return takeAsVector(0, os, incremental);
    }

    void EmptyDataset::tofile(FileFormat fmt, const URI &uri, const UDF &udf, size_t fileCount, size_t shardSize, const std::unordered_map<std::string, std::string> &outputOptions, size_t limit, std::ostream &os) {
        // nothing todo.
    }
}