//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/9/2022                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_INCREMENTALRESOLVETASK_H
#define TUPLEX_INCREMENTALRESOLVETASK_H

#include "ResolveTask.h"

namespace tuplex {
    class IncrementalResolveTask : public ResolveTask {
    public:
        IncrementalResolveTask() = delete;

        IncrementalResolveTask(int64_t stageID,
                               int64_t contextID,
                               const std::vector<Partition*>& partitions,
                               const std::vector<Partition*>& runtimeExceptions,
                               ExceptionInfo runtimeExceptionInfo,
                               const std::vector<Partition*>& inputExceptions,
                               ExceptionInfo inputExceptionInfo,
                               const std::vector<int64_t>& operatorIDsAffectedByResolvers,
                               Schema exceptionInputSchema,
                               Schema resolverOutputSchema,
                               Schema targetNormalCaseOutputSchema,
                               Schema targetGeneralCaseOutputSchema,
                               bool mergeRows,
                               bool allowNumericTypeUnification,
                               FileFormat outputFormat,
                               char csvDelimiter,
                               char csvQuotechar,
                               codegen::resolve_f functor,
                               PyObject* interpreterFunctor) : ResolveTask::ResolveTask(stageID,
                                                                                                contextID,
                                                                                                partitions,
                                                                                                runtimeExceptions,
                                                                                                runtimeExceptionInfo,
                                                                                                inputExceptions,
                                                                                                inputExceptionInfo,
                                                                                                operatorIDsAffectedByResolvers,
                                                                                                exceptionInputSchema,
                                                                                                resolverOutputSchema,
                                                                                                targetNormalCaseOutputSchema,
                                                                                                targetGeneralCaseOutputSchema,
                                                                                                mergeRows,
                                                                                                allowNumericTypeUnification,
                                                                                                outputFormat,
                                                                                                csvDelimiter,
                                                                                                csvQuotechar,
                                                                                                functor,
                                                                                                interpreterFunctor) {}
    private:

    };

}

#endif //TUPLEX_INCREMENTALRESOLVETASK_H