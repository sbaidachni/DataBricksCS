using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBricksCS.Code
{
    enum RunResultState { SUCCESS, FAILED, TIMEDOUT, CANCELED };

    enum RunLifecycleState { PENDING, RUNNING, TERMINATING, TERMINATED, SKIPPED, INTERNAL_ERROR };

    class RunState
    {
        public RunLifecycleState life_cycle_state { get; set; }

        public RunResultState result_state { get; set; }

        public string state_message { get; set; }
    }
}
