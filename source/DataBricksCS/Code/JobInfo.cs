using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBricksCS.Code
{
    class JobInfo
    {
        public int job_id { get; set; }

        public JobSettings settings { get; set; }
    }

    class JobSettings
    {
        public string name { get; set; }

        public string existing_cluster_id { get; set; }
    }
}
