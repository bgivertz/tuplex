//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Benjamin Givertz first on 1/1/2021                                                                     //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#include <physical/ExceptionTuplexSourceTaskBuilder.h>

namespace tuplex {
    namespace codegen {
        llvm::Function* ExceptionTuplexSourceTaskBuilder::build() {
            auto func = createFunctionWithInputExceptions();

            // create main loop
            createMainLoop(func);

            return func;
        }

        void ExceptionTuplexSourceTaskBuilder::processRow(llvm::IRBuilder<> &builder, llvm::Value *userData,
                                                 const FlattenedTuple &tuple,
                                                 llvm::Value *normalRowCountVar,
                                                 llvm::Value *badRowCountVar,
                                                 llvm::Value *rowNumberVar,
                                                 llvm::Value *inputRowPtr,
                                                 llvm::Value *inputRowSize,
                                                 llvm::Function *processRowFunc) {
            using namespace llvm;

            // call pipeline function, then increase normalcounter
            if(processRowFunc) {
                callProcessFuncWithHandler(builder, userData, tuple, normalRowCountVar, rowNumberVar, inputRowPtr,
                                           inputRowSize, processRowFunc);
            } else {
                Value *normalRowCount = builder.CreateLoad(normalRowCountVar, "normalRowCount");
                builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);
            }
        }

        void ExceptionTuplexSourceTaskBuilder::callProcessFuncWithHandler(llvm::IRBuilder<> &builder, llvm::Value *userData,
                                                                 const FlattenedTuple& tuple,
                                                                 llvm::Value *normalRowCountVar,
                                                                 llvm::Value *rowNumberVar,
                                                                 llvm::Value *inputRowPtr,
                                                                 llvm::Value *inputRowSize,
                                                                 llvm::Function *processRowFunc) {
            auto& context = env().getContext();
            auto pip_res = PipelineBuilder::call(builder, processRowFunc, tuple, userData, builder.CreateLoad(rowNumberVar), initIntermediate(builder));

            // create if based on resCode to go into exception block
            auto ecCode = builder.CreateZExtOrTrunc(pip_res.resultCode, env().i64Type());
            auto ecOpID = builder.CreateZExtOrTrunc(pip_res.exceptionOperatorID, env().i64Type());
            auto numRowsCreated = builder.CreateZExtOrTrunc(pip_res.numProducedRows, env().i64Type());

            // add number of rows created to output row number variable
            auto outputRowNumber = builder.CreateLoad(rowNumberVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(rowNumberVar), numRowsCreated), rowNumberVar);

            auto exceptionRaised = builder.CreateICmpNE(ecCode, env().i64Const(ecToI32(ExceptionCode::SUCCESS)));

            llvm::BasicBlock* bbPipelineOK = llvm::BasicBlock::Create(context, "pipeline_ok", builder.GetInsertBlock()->getParent());
            llvm::BasicBlock* curBlock = builder.GetInsertBlock();
            llvm::BasicBlock* bbPipelineFailed = exceptionBlock(builder, userData, ecCode, ecOpID, outputRowNumber, inputRowPtr, inputRowSize); // generate exception block (incl. ignore & handler if necessary)

            llvm::BasicBlock* lastExceptionBlock = builder.GetInsertBlock();
            llvm::BasicBlock* bbPipelineDone = llvm::BasicBlock::Create(context, "pipeline_done", builder.GetInsertBlock()->getParent());

            builder.SetInsertPoint(curBlock);
            builder.CreateCondBr(exceptionRaised, bbPipelineFailed, bbPipelineOK);

            // pipeline ok
            builder.SetInsertPoint(bbPipelineOK);
            llvm::Value *normalRowCount = builder.CreateLoad(normalRowCountVar, "normalRowCount");
            builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);

            builder.CreateBr(bbPipelineDone);

            // connect exception block to pipeline failure
            builder.SetInsertPoint(lastExceptionBlock);
            builder.CreateBr(bbPipelineDone);

            builder.SetInsertPoint(bbPipelineDone);

            // call runtime free all
            _env->freeAll(builder);
        }

        void ExceptionTuplexSourceTaskBuilder::createMainLoop(llvm::Function *read_block_func) {
            using namespace std;
            using namespace llvm;

            assert(read_block_func);

            auto& context = env().getContext();

            auto argUserData = arg("userData");
            auto argInPtr = arg("inPtr");
            auto argInSize = arg("inSize");
            auto argExpPtr = arg("expPtr");
            auto argOutNormalRowCount = arg("outNormalRowCount");
            auto argOutBadRowCount = arg("outBadRowCount");
            auto argIgnoreLastRow = arg("ignoreLastRow");

            BasicBlock *bbBody = BasicBlock::Create(context, "entry", read_block_func);

            IRBuilder<> builder(bbBody);


            // there should be a check if argInSize is 0
            // if so -> handle separately, i.e. return immediately
#warning "add here argInSize > 0 check"



            Value *outRowCountVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "outRowCountVar"); // counter for output row number (used for exception resolution)
            Value *normalRowCountVar = argOutNormalRowCount;
            Value *badRowCountVar = argOutBadRowCount;
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(argOutBadRowCount),
                                                  builder.CreateLoad(argOutNormalRowCount)), outRowCountVar);

            // get num rows to read & process in loop
            Value *numRowsVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "numRowsVar");
            Value *input_ptr = builder.CreatePointerCast(argInPtr, env().i64Type()->getPointerTo(0));
            builder.CreateStore(builder.CreateLoad(input_ptr), numRowsVar);
            // store current input ptr
            Value *currentInputPtrVar = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "ptr");
            builder.CreateStore(builder.CreateGEP(argInPtr, env().i32Const(sizeof(int64_t))), currentInputPtrVar);


            // get num exceptions
            Value *numExpRowsVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "numExpRowsVar");
            Value *numExpRowsPtr = builder.CreatePointerCast(argExpPtr, env().i64Type()->getPointerTo(0));
            builder.CreateStore(builder.CreateLoad(numExpRowsPtr), numExpRowsVar);
            // store current exception ptr
            Value *curExpPtrVar = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "expPtr");
            builder.CreateStore(builder.CreateGEP(argExpPtr, env().i32Const(sizeof(int64_t))), curExpPtrVar);
            // current exception row
            Value *curExpRowVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "expRowVar");
            builder.CreateStore(env().i64Const(0), curExpRowVar);
            // current exception accumulator
            Value *expAccVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "expAccVar");
            builder.CreateStore(env().i64Const(0), expAccVar);
            // previous normal row
            Value *prevRowNumber = builder.CreateAlloca(env().i64Type(), 0, nullptr, "prevRowNumber");

            builder.CreateAlloca(env().i8ptrType().)

            // variable for current row number...
            Value *rowVar = builder.CreateAlloca(env().i64Type(), 0, nullptr);
            builder.CreateStore(env().i64Const(0), rowVar);

            BasicBlock* bbLoopCondition = BasicBlock::Create(context, "loop_cond", read_block_func);
            BasicBlock* bbLoopBody = BasicBlock::Create(context, "loop_body", read_block_func);
            BasicBlock* bbLoopDone = BasicBlock::Create(context, "loop_done", read_block_func);

            // go from entry block to loop body
            builder.CreateBr(bbLoopBody);

            // --------------
            // loop condition
            builder.SetInsertPoint(bbLoopCondition);
            Value *row = builder.CreateLoad(rowVar, "row");
            Value* nextRow = builder.CreateAdd(env().i64Const(1), row);
            Value* numRows = builder.CreateLoad(numRowsVar, "numRows");
            builder.CreateStore(nextRow, rowVar, "row");
            auto cond = builder.CreateICmpSLT(nextRow, numRows);
            builder.CreateCondBr(cond, bbLoopBody, bbLoopDone);


            // ---------
            // loop body
            builder.SetInsertPoint(bbLoopBody);
            // decode tuple from input ptr
            FlattenedTuple ft(_env.get());
            ft.init(_inputRowType);
            Value* oldInputPtr = builder.CreateLoad(currentInputPtrVar, "ptr");
            ft.deserializationCode(builder, oldInputPtr);
            Value* newInputPtr = builder.CreateGEP(oldInputPtr, ft.getSize(builder)); // @TODO: maybe use inbounds
            builder.CreateStore(newInputPtr, currentInputPtrVar);

            // call function --> incl. exception handling
            // process row here -- BEGIN
            Value *inputRowSize = ft.getSize(builder);

            builder.CreateStore(builder.CreateLoad(outRowCountVar), prevRowNumber);

            processRow(builder, argUserData, ft, normalRowCountVar, badRowCountVar, outRowCountVar, oldInputPtr, inputRowSize, pipeline() ? pipeline()->getFunction() : nullptr);

            llvm::BasicBlock* bbExpUpdate = llvm::BasicBlock::Create(context, "exp_update", builder.GetInsertBlock()->getParent());
            auto expCond = builder.CreateICmpEQ(builder.CreateLoad(outRowCountVar), builder.CreateLoad(prevRowNumber));
            builder.CreateCondBr(expCond, bbExpUpdate, bbLoopCondition);

            builder.SetInsertPoint(bbExpUpdate);
            llvm::BasicBlock* bbIncrement = llvm::BasicBlock::Create(context, "increment", builder.GetInsertBlock()->getParent());
            llvm::BasicBlock* bbIncrementDone = llvm::BasicBlock::Create(context, "increment_done", builder.GetInsertBlock()->getParent());
            auto curExpIndPtr = builder.CreatePointerCast(builder.CreateLoad(curExpPtrVar), env().i64Type()->getPointerTo(0));
            auto incCond = builder.CreateICmpSLT(builder.CreateLoad(curExpIndPtr), builder.CreateAdd(builder.CreateLoad(normalRowCountVar), builder.CreateLoad(curExpRowVar)));
            builder.CreateCondBr(incCond, bbIncrement, bbIncrementDone);

            builder.SetInsertPoint(bbIncrement);
            auto curExpIndPtr2 = builder.CreatePointerCast(builder.CreateLoad(curExpPtrVar), env().i64Type()->getPointerTo(0));
            builder.CreateStore(builder.CreateSub(builder.CreateLoad(curExpIndPtr2), builder.CreateLoad(expAccVar)), curExpIndPtr2);
            auto curOffset = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curOffset");
            builder.CreateStore(builder.CreateLoad(builder.CreateGEP(curExpIndPtr2, env().i32Const(1))), curOffset);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curOffset), env().i64Const(2 * sizeof(int64_t))), curOffset);
            builder.CreateStore(builder.CreateGEP(builder.CreateLoad(curExpPtrVar), builder.CreateLoad(curOffset)), curExpPtrVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curExpRowVar), env().i64Const(1)), curExpRowVar);
            builder.CreateBr(bbExpUpdate);

            builder.SetInsertPoint(bbIncrementDone);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(expAccVar), env().i64Const(1)), expAccVar);
            builder.CreateBr(bbLoopCondition);

            // ---------
            // loop done
            builder.SetInsertPoint(bbLoopDone);

            llvm::BasicBlock* bbRemainingExceptions = llvm::BasicBlock::Create(context, "remaining_exceptions", builder.GetInsertBlock()->getParent());
            llvm::BasicBlock* bbRemainingDone = llvm::BasicBlock::Create(context, "remaining_done", builder.GetInsertBlock()->getParent());
            auto expRemaining = builder.CreateICmpSLT(builder.CreateLoad(curExpRowVar), builder.CreateLoad(numExpRowsVar));
            builder.CreateCondBr(expRemaining, bbRemainingExceptions, bbRemainingDone);

            builder.SetInsertPoint(bbRemainingExceptions);
            auto curExpIndPtr3 = builder.CreatePointerCast(builder.CreateLoad(curExpPtrVar), env().i64Type()->getPointerTo(0));
            builder.CreateStore(builder.CreateSub(builder.CreateLoad(curExpIndPtr3), builder.CreateLoad(expAccVar)), curExpIndPtr3);
            auto curOffset2 = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curOffset2");
            builder.CreateStore(builder.CreateLoad(builder.CreateGEP(curExpIndPtr3, env().i32Const(1))), curOffset2);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curOffset2), env().i64Const(2 * sizeof(int64_t))), curOffset2);
            builder.CreateStore(builder.CreateGEP(builder.CreateLoad(curExpPtrVar), builder.CreateLoad(curOffset2)), curExpPtrVar);
            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(curExpRowVar), env().i64Const(1)), curExpRowVar);
            builder.CreateBr(bbLoopDone);

            builder.SetInsertPoint(bbRemainingDone);
            // if intermediate callback desired, perform!
            if(_intermediateType != python::Type::UNKNOWN && !_intermediateCallbackName.empty()) {
                writeIntermediate(builder, argUserData, _intermediateCallbackName);
            }

            env().storeIfNotNull(builder, builder.CreateLoad(normalRowCountVar), argOutNormalRowCount);
            env().storeIfNotNull(builder, builder.CreateLoad(badRowCountVar), argOutBadRowCount);

            // return bytes read
            Value* curPtr = builder.CreateLoad(currentInputPtrVar, "ptr");
            Value* bytesRead = builder.CreateSub(builder.CreatePtrToInt(curPtr, env().i64Type()), builder.CreatePtrToInt(argInPtr, env().i64Type()));
            builder.CreateRet(bytesRead);
        }
    }

//        void ExceptionTuplexSourceTaskBuilder::processRow(llvm::IRBuilder<> &builder, llvm::Value *userData,
//                                                 const FlattenedTuple &tuple,
//                                                 llvm::Value *normalRowCountVar,
//                                                 llvm::Value *badRowCountVar,
//                                                 llvm::Value *rowNumberVar,
//                                                 llvm::Value *inputRowPtr,
//                                                 llvm::Value *inputRowSize,
//                                                 llvm::Function *processRowFunc) {
//            using namespace llvm;
//
//            // call pipeline function, then increase normalcounter
//            if(processRowFunc) {
//                callProcessFuncWithHandler(builder, userData, tuple, normalRowCountVar, rowNumberVar, inputRowPtr,
//                                           inputRowSize, processRowFunc);
//            } else {
//                Value *normalRowCount = builder.CreateLoad(normalRowCountVar, "normalRowCount");
//                builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);
//            }
//        }
//
//        void ExceptionTuplexSourceTaskBuilder::callProcessFuncWithHandler(llvm::IRBuilder<> &builder, llvm::Value *userData,
//                                                                 const FlattenedTuple& tuple,
//                                                                 llvm::Value *normalRowCountVar,
//                                                                 llvm::Value *rowNumberVar,
//                                                                 llvm::Value *inputRowPtr,
//                                                                 llvm::Value *inputRowSize,
//                                                                 llvm::Function *processRowFunc) {
//            auto& context = env().getContext();
//            auto pip_res = PipelineBuilder::call(builder, processRowFunc, tuple, userData, builder.CreateLoad(rowNumberVar), initIntermediate(builder));
//
//            // create if based on resCode to go into exception block
//            auto ecCode = builder.CreateZExtOrTrunc(pip_res.resultCode, env().i64Type());
//            auto ecOpID = builder.CreateZExtOrTrunc(pip_res.exceptionOperatorID, env().i64Type());
//            auto numRowsCreated = builder.CreateZExtOrTrunc(pip_res.numProducedRows, env().i64Type());
//
//            // add number of rows created to output row number variable
//            auto outputRowNumber = builder.CreateLoad(rowNumberVar);
//            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(rowNumberVar), numRowsCreated), rowNumberVar);
//
//            auto exceptionRaised = builder.CreateICmpNE(ecCode, env().i64Const(ecToI32(ExceptionCode::SUCCESS)));
//
//            llvm::BasicBlock* bbPipelineOK = llvm::BasicBlock::Create(context, "pipeline_ok", builder.GetInsertBlock()->getParent());
//            llvm::BasicBlock* curBlock = builder.GetInsertBlock();
//            llvm::BasicBlock* bbPipelineFailed = exceptionBlock(builder, userData, ecCode, ecOpID, outputRowNumber, inputRowPtr, inputRowSize); // generate exception block (incl. ignore & handler if necessary)
//
//            llvm::BasicBlock* lastExceptionBlock = builder.GetInsertBlock();
//            llvm::BasicBlock* bbPipelineDone = llvm::BasicBlock::Create(context, "pipeline_done", builder.GetInsertBlock()->getParent());
//
//            builder.SetInsertPoint(curBlock);
//            builder.CreateCondBr(exceptionRaised, bbPipelineFailed, bbPipelineOK);
//
//            // pipeline ok
//            builder.SetInsertPoint(bbPipelineOK);
//            llvm::Value *normalRowCount = builder.CreateLoad(normalRowCountVar, "normalRowCount");
//            builder.CreateStore(builder.CreateAdd(normalRowCount, env().i64Const(1)), normalRowCountVar);
//
//            builder.CreateBr(bbPipelineDone);
//
//            // connect exception block to pipeline failure
//            builder.SetInsertPoint(lastExceptionBlock);
//            builder.CreateBr(bbPipelineDone);
//
//
//            builder.SetInsertPoint(bbPipelineDone);
//            llvm::BasicBlock* bbExpUpdate = llvm::BasicBlock::Create(context, "exp_update", builder.GetInsertBlock()->getParent());
//            llvm::BasicBlock* bbExpUpdateDone = llvm::BasicBlock::Create(context, "exp_update_done", builder.GetInsertBlock()->getParent());
//            auto numRows = builder.CreateZExtOrTrunc(pip_res.numProducedRows, env().i64Type());
//            auto shouldUpdate = builder.CreateICmpEQ(numRows, env().i64Const(0));
//            builder.CreateCondBr(shouldUpdate, bbExpUpdate, bbExpUpdateDone);
//
//            builder.SetInsertPoint(bbExpUpdate);
//            llvm::BasicBlock* bbIncrement = llvm::BasicBlock::Create(context, "increment", builder.GetInsertBlock()->getParent());
//            llvm::BasicBlock* bbIncrementDone = llvm::BasicBlock::Create(context, "increment_done", builder.GetInsertBlock()->getParent());
//
//
//            auto currentIndVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "currentIndVar");
//            auto currentIndPtr = builder.CreatePointerCast(expPtrVar, env().i64Type()->getPointerTo(0));
//            builder.CreateStore(builder.CreateLoad(currentIndPtr), currentIndVar);
//
//            auto currentNormalCount = builder.CreateLoad(normalRowCountVar, "currentNormalCount");
//
//            auto shouldIncrement = builder.CreateICmpSLT(currentIndVar, currentNormalCount);
//            builder.CreateCondBr(shouldIncrement, bbIncrement, bbIncrementDone);
//
//
//            builder.SetInsertPoint(bbIncrement);
//            auto curAcc = builder.CreateLoad(expAccVar);
//
//            auto curInd = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curInd");
//            auto curIndPtr = builder.CreatePointerCast(expPtrVar, env().i64Type()->getPointerTo(0));
//            builder.CreateStore(builder.CreateLoad(curIndPtr), curInd);
//
//            // TODO
//
//            builder.CreateBr(bbIncrementDone);
//
//            builder.SetInsertPoint(bbIncrementDone);
//            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(expAccVar), env().i64Const(1)), expAccVar);
//            builder.CreateBr(bbExpUpdateDone);
//
//
//            builder.SetInsertPoint(bbExpUpdateDone);
//            // call runtime free all
//            _env->freeAll(builder);
//
////            auto exceptionIndVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "exceptionIndVar");
////            auto exceptionIndPtr = builder.CreatePointerCast(expPtrVar, env().i64Type()->getPointerTo(0));
////            builder.CreateStore(builder.CreateLoad(exceptionIndPtr), exceptionIndVar);
////
////            llvm::BasicBlock* bbIncrement = llvm::BasicBlock::Create(context, "incremenent", builder.GetInsertBlock()->getParent());
////            llvm::BasicBlock* bbIncrementDone = llvm::BasicBlock::Create(context, "increment_done", builder.GetInsertBlock()->getParent());
////
////            llvm::Value *exceptionRowCount = builder.CreateLoad(exceptionIndVar, "exceptionRowCount");
////            llvm::Value *normalRowCount2 = builder.CreateLoad(normalRowCountVar, "normalRowCount");
////            auto cond = builder.CreateICmpSLT(exceptionRowCount, normalRowCount2);
////            builder.CreateCondBr(cond, bbIncrement, bbIncrementDone);
//////            builder.CreateCondBr(cond, bbIncrementDone, bbIncrementDone);
////
////
////            builder.SetInsertPoint(bbIncrement);
////            auto curValPtr = builder.CreatePointerCast(expPtrVar, env().i64Type()->getPointerTo(0));
////            auto curVal = builder.CreateAlloca(env().i64Type(), 0, nullptr, "curVal");
////            builder.CreateStore(builder.CreateLoad(curValPtr), curVal);
////
////            auto curAcc = builder.CreateLoad(expAccVar, "curAcc");
//////                auto newPtr = builder.CreateAlloca(env().i8ptrType(), 0, nullptr, "newPtr");
//////            builder.CreateStore(builder.CreateSub(curVal, curAcc), newPtr);
////
////            auto newVal = builder.CreateSub(builder.CreateLoad(curVal), curAcc);
//////
//////            builder.CreateStore(newPtr, expPtrVar);
////
//////            auto exceptionIndVar2 = builder.CreateAlloca(env().i64Type(), 0, nullptr, "exceptionIndVar");
//////            auto exceptionIndPtr2 = builder.CreatePointerCast(expPtrVar, env().i64Type()->getPointerTo(0));
//////            builder.CreateStore(builder.CreateLoad(exceptionIndPtr2), exceptionIndVar2);
//////            builder.CreateStore(builder.CreateSub(exceptionIndVar2, expAccVar), exceptionIndPtr2);
//////            builder.CreateStore(builder.CreateAdd(expPtrVar, env().i64Const(sizeof(int64_t))), expPtrVar);
//////            builder.CreateStore(builder.CreateAdd(expPtrVar, builder.CreateLoad(builder.CreatePointerCast(expPtrVar, env().i64Type()->getPointerTo(0)))), expPtrVar);
//////            builder.CreateStore(builder.CreateAdd(expPtrVar, env().i64Const(sizeof(int64_t))), expPtrVar);
//////            builder.CreateStore(builder.CreateAdd(expRowVar, env().i64Const(1)), expRowVar);
////            builder.CreateBr(bbIncrementDone);
////
////            builder.SetInsertPoint(bbIncrementDone);
////            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(expAccVar), env().i64Const(1)), expAccVar);
////            builder.CreateBr(bbExpUpdateDone);
////
//////            auto rowFiltered = builder.CreateICmpEQ(numRowsCreated, env().i64Const(0));
//////
//////          llvm::BasicBlock* bbRowFiltered = llvm::BasicBlock::Create(context, "row_filtered", builder.GetInsertBlock()->getParent());
////
////
////            builder.SetInsertPoint(bbExpUpdateDone);
//
////
////            builder.SetInsertPoint(bbRowFiltered);
////            builder.CreateBr(bbRowNotFiltered);
////            auto exceptionIndVar = builder.CreateAlloca(env().i64Type(), 0, nullptr, "exceptionIndVar");
////            auto exceptionIndPtr = builder.CreatePointerCast(expPtrVar, env().i64Type()->getPointerTo(0));
////            builder.CreateStore(builder.CreateLoad(exceptionIndPtr), exceptionIndVar);
////
////            llvm::BasicBlock* bbIncrement = llvm::BasicBlock::Create(context, "incremenent", builder.GetInsertBlock()->getParent());
////            llvm::BasicBlock* bbIncrementDone = llvm::BasicBlock::Create(context, "increment_done", builder.GetInsertBlock()->getParent());
////
////            llvm::Value *exceptionRowCount = builder.CreateLoad(exceptionIndVar, "exceptionRowCount");
////            auto cond = builder.CreateICmpSLT(exceptionRowCount, normalRowCount);
////            builder.CreateCondBr(cond, bbIncrement, bbIncrementDone);
////
////            builder.SetInsertPoint(bbIncrement);
////            builder.CreateStore(builder.CreateSub(exceptionIndVar, expAccVar), exceptionIndPtr);
////            builder.CreateStore(builder.CreateAdd(expPtrVar, env().i64Const(sizeof(int64_t))), expPtrVar);
////            builder.CreateStore(builder.CreateAdd(expPtrVar, builder.CreateLoad(builder.CreatePointerCast(expPtrVar, env().i64Type()->getPointerTo(0)))), expPtrVar);
////            builder.CreateStore(builder.CreateAdd(expPtrVar, env().i64Const(sizeof(int64_t))), expPtrVar);
////            builder.CreateStore(builder.CreateAdd(expRowVar, env().i64Const(1)), expRowVar);
////
////            builder.SetInsertPoint(bbIncrementDone);
////            builder.CreateStore(builder.CreateAdd(builder.CreateLoad(expAccVar), env().i64Const(1)), expAccVar);
////
////            builder.SetInsertPoint(bbRowNotFiltered);
//
//
//        }

}