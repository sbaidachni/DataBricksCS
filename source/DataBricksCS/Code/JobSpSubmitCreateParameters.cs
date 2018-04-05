using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBricksCS.Code
{
    class JobSpSubmitCreateParameters
    {
        public ClusterCreateParameters new_cluster { get; set; }

        public string name { get; set; }

        public SparkSubmitTask spark_submit_task { get; set; }

        public int max_concurrent_runs { get; set; }
    }
}
