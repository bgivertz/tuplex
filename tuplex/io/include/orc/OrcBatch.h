//--------------------------------------------------------------------------------------------------------------------//
//                                                                                                                    //
//                                      Tuplex: Blazing Fast Python Data Science                                      //
//                                                                                                                    //
//                                                                                                                    //
//  (c) 2017 - 2021, Tuplex team                                                                                      //
//  Created by Ben Givertz first on 8/31/2021                                                                         //
//  License: Apache 2.0                                                                                               //
//--------------------------------------------------------------------------------------------------------------------//

#ifndef TUPLEX_ORCBATCH_H
#define TUPLEX_ORCBATCH_H

namespace tuplex { namespace orc {

/*!
 * Interface for reading and writing to Orc batches from Tuplex fields
 */
        class OrcBatch {
        public:
            /*!
             * destructor must ensure all child batches are destroyed.
             */
            virtual ~OrcBatch() = default;

            /*!
             * sets the the data in a row of an Orc batch from a tuplecx field.
             * @param field
             * @param rowIndex
             */
            virtual void setData(tuplex::Field field, uint64_t rowIndex) = 0;
        };

    }}

#endif //TUPLEX_ORCBATCH_H
