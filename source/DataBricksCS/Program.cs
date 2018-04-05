using DatabricksCSDemo.Code;
using DataBricksCS.Code;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace DataBricksCS
{
    class Program
    {
        static string tokenSecret = "{provide your key}";

        //case sensitive
        static string clusterName = "sbaydachcluster2";
        static string jobName = "sbaydachjob";
        static string sparkVersionName = "3.5.x-scala2.11";
        static string nodeTypeName = "Standard_DS3_v2";
   
        static async Task Main(string[] args)
        {
            //variables to store internal information
            ClusterInfo myCluster = null;
            JobInfo myJob = null;
            SparkVersion myVersion = null;
            NodeType myNodeType = null;
            int job_id = 0;
            int run_id = 0;
            RunsInfo myRun = null;

            ///Checking spark versions
            Console.WriteLine("Checking available spark versions...");
            var versions=await DatabricksMethods.GetSparkVersions(tokenSecret);
            myVersion = (from a in versions.versions where a.key.Equals(sparkVersionName)
                         select a).FirstOrDefault();
            
            if (myVersion==null)
            {
                Console.WriteLine($"The version {sparkVersionName} is not found");
                return;
            }
            Console.WriteLine($"The version {sparkVersionName} is applied");


            //Checking Node types
            Console.WriteLine("Checking available spark node types...");
            var nodeTypes = await DatabricksMethods.GetSparkNodes(tokenSecret);

            myNodeType = (from a in nodeTypes.node_types
                         where a.node_type_id.Equals(nodeTypeName)
                         select a).FirstOrDefault();

            if (myNodeType == null)
            {
                Console.WriteLine($"The type {nodeTypeName} is not found");
                return;
            }
            Console.WriteLine($"The type {nodeTypeName} is applied");


            //creating a new cluster
            ClusterCreateParameters par = new ClusterCreateParameters()
            {
                cluster_name= clusterName,
                num_workers=1,
                spark_version=sparkVersionName,
                node_type_id=nodeTypeName
            };

            await DatabricksMethods.CreateCluster(par, tokenSecret);
            Console.WriteLine("Cluster is created");

            //Getting cluster list
            /*var list=await DatabricksMethods.GetClusterList(tokenSecret);
            myCluster=(from a in list.clusters where a.cluster_name.Equals(clusterName) select a).FirstOrDefault();

            if (myCluster==null)
            {
                Console.WriteLine("The cluster is not available");
                return;
            }

            if (myCluster.state!=ClusterState.TERMINATED)
            {
                Console.WriteLine($"The cluster has a wron state:{myCluster.state}");
                return;
            }*/

            //Starting a cluster
            /*Console.WriteLine("Starting the cluster");
            await DatabricksMethods.StartCluster(myCluster.cluster_id, tokenSecret);

            do
            {
                myCluster = await DatabricksMethods.GetClusterInfo(
                    myCluster.cluster_id, tokenSecret);
                Console.WriteLine($"The current status: {myCluster.state}");
                if (myCluster.state==ClusterState.ERROR)
                {
                    Console.WriteLine($"The cluster cannot be started");
                    return;
                }
                Thread.Sleep(5000);

            }
            while (myCluster.state != ClusterState.RUNNING);*/


            //Getting list of jobs and create a new job
            /*Console.WriteLine("Checking the job");
            var jobList=await DatabricksMethods.GetJobsList(tokenSecret);
            if (jobList.jobs!=null)
                myJob = (from a in jobList.jobs where a.settings.name.Equals(jobName) select a).FirstOrDefault();
            
            if (myJob == null)
            {
                Console.WriteLine("Creating the job");

                JobSpSubmitCreateParameters jobPar = new JobSpSubmitCreateParameters()
                {
                    max_concurrent_runs = 10,
                    name = jobName,
                    new_cluster = new ClusterCreateParameters()
                    {
                        cluster_name = String.Empty,
                        node_type_id = nodeTypeName,
                        num_workers = 1,
                        spark_version = sparkVersionName
                    },
                    spark_submit_task = new SparkSubmitTask()
                    {
                        parameters = new List<string>()
                        {
                            "--py-files",
                            "jobs.zip main.py"
                        }
                    }
                };

                job_id=await DatabricksMethods.CreateJob(jobPar, tokenSecret);
                myJob = await DatabricksMethods.GetJobInfo(job_id,tokenSecret);
            }
            Console.WriteLine("The job is available");

            //submitting the job
            run_id=await DatabricksMethods.RunJobNow(myJob.job_id, tokenSecret);
            Console.WriteLine("Job is running");

            myRun = await DatabricksMethods.GetRun(run_id,tokenSecret);

            while(myRun.state.life_cycle_state!=RunLifecycleState.TERMINATED)
            {
                Console.WriteLine($"Run state is {myRun.state.life_cycle_state}");
                Thread.Sleep(5000);
                myRun = await DatabricksMethods.GetRun(run_id, tokenSecret);
            }

            //Terminate a cluster
            /*Console.WriteLine("Terminating the cluster");
            await DatabricksMethods.TerminateCluster(myCluster.cluster_id, tokenSecret);
            do
            {
                myCluster = await DatabricksMethods.GetClusterInfo(
                    myCluster.cluster_id, tokenSecret);
                Console.WriteLine($"The current status: {myCluster.state}");
            }
            while (myCluster.state != ClusterState.TERMINATED);
            */
            Console.ReadLine();
        }
    }
}
