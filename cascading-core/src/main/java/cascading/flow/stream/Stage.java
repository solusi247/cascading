/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cascading.flow.stream;

import cascading.operation.BaseOperation;

/**
 *
 */
public class Stage<Incoming, Outgoing> extends Duct<Incoming, Outgoing>
  {
  public void receive( Duct previous, Incoming incoming )
    {
    if (this.flowProcess!=null & this.pipe!=null){
       //System.out.println(this.getClass().getName()+" recive :"+this.getPipe().getLabel());
       this.flowProcess.increment(this.getPipe().getLabel(), BaseOperation.INPUTRECORD, 1);
       this.flowProcess.increment(this.getPipe().getLabel(), BaseOperation.OUTPUTRECORD, 1);
    }
    next.receive( this, (Outgoing) incoming );
    }
  }
