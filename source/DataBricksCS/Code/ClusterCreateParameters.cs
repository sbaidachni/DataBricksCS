using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataBricksCS.Code
{
    class ClusterCreateParameters
    {
        public int num_workers { get; set; }

        public string cluster_name { get; set; }

        public string spark_version { get; set; }

        public string node_type_id { get; set; }
    }
}
