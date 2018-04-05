using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DatabricksCSDemo.Code
{
    enum ClusterState { PENDING, RUNNING, RESTARTING, RESIZING, TERMINATING, TERMINATED, ERROR, UNKNOWN };

    class ClusterInfo
    {
        public string cluster_id { get; set; }

        public string cluster_name { get; set; }

        public ClusterState state { get; set; }
    }
}
