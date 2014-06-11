package MapRedClasses;




import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;

public class Main {
	
	public static final String LOCAL_PRIME_LIST = "/home/user/hadoop-1.0.4/projects/ParallelFim/src/primes-to-100k.txt";
	public static final String HDFS_PRIME_LIST = "/app/hadoop/primes-to-100k.txt";
	static int support=1000;
	
	
	public static class FimMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, LongWritable>
	{
		private final static Text itemset = new Text(); 
		private final static LongWritable primes = new LongWritable();
		
		
		private Path[] localFiles;
		static int[] primenos = new int[550];
		//for graph
		SimpleWeightedGraph<String,DefaultEdge> g,g1,g4;
		List<String> dcn[],dcn1[],dcn2[];
		List<Object> answer[];
		Set<String> vset;
		int pushed[]=null;
		
//		double[] prime={2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101,103,107,109,113,127,131};
	//	int support=1;
	     
	    Stack<String> root_stack = new Stack<String>();
	  	Stack<String> nodes_stack = new Stack<String>();
	  	double w0 = 1;
	  	double w1 = 1;
		
		//end for graph
		
		public void configure(JobConf job)				//get the cached files
		{
			try {
				
				localFiles = DistributedCache.getLocalCacheFiles(job);
				System.out.println(localFiles[0].toString());
				int i=0;
				BufferedReader primeread = new BufferedReader(new FileReader(localFiles[0].toString()));
				String primeline;
				while(i<550 && ((primeline = primeread.readLine())!=null))
				{
					primenos[i]=Integer.parseInt(primeline);
					i++;
				}
				primeread.close();
				
				//create graph object
		        g = new SimpleWeightedGraph<String, DefaultEdge>(DefaultWeightedEdge.class);        
		        g1 = new SimpleWeightedGraph<String, DefaultEdge>(DefaultWeightedEdge.class);


				
			//	System.out.println(localFiles[0].toString());
			} catch (IOException e) {
				
				System.out.println("DistributedCache.getLocalCacheFiles exception");
				e.printStackTrace();
			}
		}
		
		
		private List[] createDCNList(SimpleWeightedGraph<String, DefaultEdge> g12) {
			
			vset= g12.vertexSet();
	        System.out.println("Total Vertex : "+vset.size());      
	        
	        Iterator<String> iter = vset.iterator();
	        List<String> dCN[] = new ArrayList[vset.size()];
	       
			for(int i=0;iter.hasNext();i++)
		       {
		    	   dCN[i] = new ArrayList<String>();
		    	   dCN[i].add(iter.next());
		    	   Set<DefaultEdge> edges = g12.edgesOf(dCN[i].get(0));
		    	   
		    	   Iterator<DefaultEdge> edgesiter = edges.iterator();
		    	   while(edgesiter.hasNext())
		    	   {
		     			DefaultEdge tempedge = edgesiter.next();
		     			String src = g12.getEdgeSource(tempedge);
		     			if(src.equals(dCN[i].get(0)))
		     				dCN[i].add(g12.getEdgeTarget(tempedge));
		     			else
		     				dCN[i].add(g12.getEdgeSource(tempedge));     			
		    	   }

		       }
			return dCN;
		}
		
		
		private void drawGraph(SimpleWeightedGraph<String, DefaultEdge> g2) throws Exception 
		{
			
			StringTokenizer itr_over_line = new StringTokenizer(line, ";");
			
			while(itr_over_line.hasMoreTokens())
			{
				StringTokenizer itr_over_items = new StringTokenizer(itr_over_line.nextToken());
				String ver=null,nextver;
				ArrayList<String> transList = new ArrayList<String>();
			
				while(itr_over_items.hasMoreTokens())
				{
					ver = itr_over_items.nextToken();
					if(!transList.contains(ver))
						transList.add(ver);
	    		
					if(!g2.containsVertex(ver))          //add all the vertex in graph
						g2.addVertex(ver);
				
		//		itemset.set(itr.nextToken());
		//		primes.set(primenos[ind]);
				
		//		System.out.println(itemset.toString()+"::"+primes);
				//output.collect(item, primes);
				}
			
		//	ind++;
			
				int prime = primenos[ind];
			
				for(int j=0;j<transList.size()-1;j++)
				{
        	//	System.out.println(transList.toString()+transList.size());
					if(j!=transList.size()-1)
					{
						int temp=j+1;
						while(temp!=transList.size())
						{
							ver=transList.get(j);
							nextver = transList.get(temp);
        				
							boolean e=g2.containsEdge(ver,nextver);	
							if(e)
							{	
								g2.setEdgeWeight(g2.getEdge(ver,nextver), g2.getEdgeWeight1big(g2.getEdge(ver,nextver)).multiply(new BigDecimal(prime)), g2.getEdgeWeight2(g2.getEdge(ver,nextver))+1);
//        					System.out.println(g.getEdge(ver,nextver)+": "+g.getEdgeWeight1big(g.getEdge(ver,nextver)));
							}	
							else
							{
								g2.addEdge(ver,nextver);
      //  					System.out.println(prime);
								g2.setEdgeWeight(g2.getEdge(ver,nextver), new BigDecimal(prime), 1);
							}
        					
		//					System.out.println(g2.getEdge(ver,nextver)+"  "+g2.getEdgeWeight1big(g2.getEdge(ver,nextver))+"  "+g2.getEdgeWeight2(g2.getEdge(ver,nextver)));
							temp++;
						}
					}
        		
				}
			
			ind++;
			}
		}
		

		private List[] createFilteredList(SimpleWeightedGraph<String, DefaultEdge> g12) 
		{
			Set<String> vset1= g12.vertexSet();
		//	System.out.println("Total Vertex-no pruning: "+vset.size());      
	        
			Iterator<String> iter = vset.iterator();
	        List<String> dCN[] = new ArrayList[vset1.size()];
		        
		        for(int i=0;iter.hasNext();i++)
		        {
		    	   dCN[i] = new ArrayList<String>();
		    	   dCN[i].add(iter.next());
		 //   	   System.out.print("For "+dCN[i].get(0)+" node: ");
		    	   Set<DefaultEdge> edgesofroot = g12.edgesOf(dCN[i].get(0));
		    	   
		    	   Iterator<DefaultEdge> edgesiter = edgesofroot.iterator();
		    	   while(edgesiter.hasNext())
		    	   {
		     			DefaultEdge tempedge = edgesiter.next();
		  //   			System.out.print(tempedge);
		     			
		     			String src = g12.getEdgeSource(tempedge);
		     			boolean flag=false;
		     			
		     			if(src.equals(dCN[i].get(0)))
		     			{
		     				for(int m=0;m<i;m++)
		     				{
		     					if(g12.getEdgeTarget(tempedge).equals(dCN[m].get(0)))
		     					{
		     						flag = true;
		     					}
		     				}     				
		     				
		     				if(flag==false)
		     					dCN[i].add(g12.getEdgeTarget(tempedge));
		     			}
		     			
		     			else
		     			{
		     				for(int m=0;m<i;m++)
		     				{
		     					if(src.equals(dCN[m].get(0)))
		     					{
		     						flag = true;
		     					}
		     				}	
		     				
		     				if(flag==false)
		     					dCN[i].add(src);
		
		     			}

		    	   }
		   // 	  System.out.println(); 
		   // 	  System.out.println(dCN[i]);
		       
		       }	
		        return dCN;
		}
		
		
		
		String line=null;
		int ind=0;
		
		public void map(LongWritable key, Text value,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException 
		{
			
			
			System.out.println("in map method");
			
			FileSplit fileSplit = (FileSplit)reporter.getInputSplit();
			System.out.println(fileSplit.toString());
		//	graphroutine;
			
			
			line = value.toString();
			
		//	System.out.println(line);
			
			try {
				drawGraph(g);
			} catch (Exception e1) {
				System.out.println("Exception from drawgraph");
				e1.printStackTrace();
			}
			
			
	//		System.out.println("Original graph: "+g);
	//		System.out.println("---------------------------------------------------------------------");
			
			//create Directly Connected List for nodes from original graph
	        dcn = createDCNList(g);
		    System.out.println("Directly Connected List from original graph: dcn list: ");
	/*	    for(int i=0;i<dcn.length;i++)
	        	System.out.println(dcn[i]);
	        System.out.println("---------------------------------------------------------------------");
	*/

	 	       //  removing repeating nodes for traversal

	        System.out.println("dcn2 List -> removing repeating nodes for traversal:");
	 	    dcn2 = createFilteredList(g);

	 //	    System.out.println("---------------------------------------------------------------------");
		
	 	   //Stack Traversal
	 	    answer = new ArrayList[dcn2.length];
	 	    for(int i=0;i<dcn2.length;i++)
	 	    {
	 	    	   answer[i] = new ArrayList();
	 	    	   
	 	    	   answer[i] = prepareStacks(dcn2[i],output);
	 
	 	//    	   System.out.println("========================================================================");
	 	 //   	   System.out.println("Answer from node : "+dcn2[i].get(0)+" xxxxxxxxxxx "+answer[i]);
	 	//    	   System.out.println("========================================================================");
	 	    	   
	 	    	  root_stack.clear();
		    	  nodes_stack.clear();
	 	    }
	 	    
		}
		
		
		String topofrootstack,topofnodestack;
		BigDecimal prev_w1 = new BigDecimal(1);
		double measured_sup;
		
		
		private List<Object> prepareStacks(List dcn12, OutputCollector<Text, LongWritable> output)
			{	
			
				List<Object> ans = new ArrayList();
				List<String> list1 = dcn12;
				String node1 = list1.get(0);
				root_stack.push(node1);
				nodes_stack.push(node1);
				
				for(int i=1;i<list1.size();i++)
					nodes_stack.push(list1.get(i).toString());
				
		//		System.out.println(root_stack+"::::"+nodes_stack);
				
	CheckCondition:
				for(;;)
				{	
					
					topofrootstack = root_stack.peek();
					topofnodestack = nodes_stack.peek();
		//			System.out.println("Read top of root and nodes stack:"+root_stack+":::"+nodes_stack);
					
		//			System.out.println("Check edge for top elements: ("+topofrootstack+","+topofnodestack+") ");
					
					boolean flag = checkEdge(topofrootstack,topofnodestack);
					if(flag)
					{
							List<String> listoftop_nodestack = searchListForTopOnNodeStack(topofnodestack);
							root_stack.push(topofnodestack);
							
							for(int i=1;i<listoftop_nodestack.size();i++)
								nodes_stack.push(listoftop_nodestack.get(i));
							
			//				System.out.println("Condition satisfied: status: "+root_stack+":::"+nodes_stack);
						
							boolean same;
			//				System.out.println("Check top of stacks: if same then add rootstack to answer and pop. ");
							same = checkTopOfStacks();
							
							List<String> ans1; //temporary
							while(same)
							{
								if(root_stack.size()!=1)
								{	
									ans.addAll(root_stack); //main array returned to function call

									ans1 = new ArrayList<String>();
									ans1.addAll(root_stack);									
									
									BigDecimal old_gcd_from_rootstack = getOldGCDFromRoot_stack(root_stack);
									measured_sup = computeGCD.factorizenew(old_gcd_from_rootstack);
									//sorted output
									Collections.sort(ans1);
								//	String itemstring = ListToString(ans1); 
									
									itemset.set(ans1.toString());
									primes.set((long)measured_sup);
									
									ans.add(measured_sup);
									ans.add("$");							
									
									
									ans1.clear();
				/*					
									System.out.println();
									System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
									System.out.println(itemset+" count:"+primes);
									System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
					*/				
							//output to combiner		
									try {
										output.collect(itemset, primes);
									} catch (IOException e) {
									System.out.println("Outputcollector exception in mapping:");
										e.printStackTrace();
									}
		
								//	System.out.println("-------------------------------> Part Answer: "+ans);
								}
								
								root_stack.pop();
								nodes_stack.pop();
					//			System.out.println("ChecktopOfStacks satisfied: poped: status: "+root_stack+"::::"+nodes_stack);
								if(nodes_stack.isEmpty() || root_stack.isEmpty())
									break;
					//			System.out.println("Again check top of stacks");
								same = checkTopOfStacks();
							}
					
						
				//			System.out.println(root_stack+":::"+nodes_stack);
						
							if(nodes_stack.isEmpty() || root_stack.isEmpty())
								break;
						
							continue CheckCondition;
					}
					else
					{
							if(!topofrootstack.equals(topofnodestack))
							{
								nodes_stack.pop();
								
								boolean same;
//								System.out.println("Check top of stacks: if same then add rootstack to answer and pop. ");
								same = checkTopOfStacks();
								
								List<String> ans1;
								while(same)
								{
									if(root_stack.size()!=1)
									{	
										ans.addAll(root_stack); //main array returned to function call

										ans1 = new ArrayList<String>();
										ans1.addAll(root_stack);									
										
										BigDecimal old_gcd_from_rootstack = getOldGCDFromRoot_stack(root_stack);
										measured_sup = computeGCD.factorizenew(old_gcd_from_rootstack);
										//sorted output
										Collections.sort(ans1);
									//	String itemstring = ListToString(ans1); 
										
										itemset.set(ans1.toString());
										primes.set((long)measured_sup);
										
										ans.add(measured_sup);
										ans.add("$");							
										
										
										ans1.clear();
	/*									
										System.out.println();
										System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
										System.out.println(itemset+" count:"+primes);
										System.out.println("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
		*/					
										//output to combiner		
							
										try {
											output.collect(itemset, primes);
										} catch (IOException e) {
										System.out.println("Outputcollector exception in mapping:");
											e.printStackTrace();
										}
									
									
									}
									
									
									root_stack.pop();
									nodes_stack.pop();
			//						System.out.println("ChecktopOfStacks satisfied: poped: status: "+root_stack+"::::"+nodes_stack);
									if(nodes_stack.isEmpty() || root_stack.isEmpty())
										break;
			//						System.out.println("Again check top of stacks");
									same = checkTopOfStacks();
								}
							}
							else
							{
								root_stack.pop();
								nodes_stack.pop();
							}
							
							if(nodes_stack.isEmpty() || root_stack.isEmpty())
								break;
						
							continue CheckCondition;
		
					}
				}
				
				
			return ans;
		}

		
		BigDecimal new_w1 = new BigDecimal(1);
		
		private boolean checkEdge(String topofrootstack2, String topofnodestack2) 
		{

			if(topofrootstack2.equals(topofnodestack2))
				return false;
			DefaultEdge edge = new DefaultEdge();
			edge = g.getEdge(topofrootstack2, topofnodestack2);
			new_w1 = g.getEdgeWeight1big(edge);
			
		//	System.out.println("("+topofrootstack2+","+topofnodestack2+") new_w1: "+new_w1);
			//System.out.printf("(%s,%s) new_w1: %2.2e \n",topofrootstack2,topofnodestack2,new_w1);
		//	System.out.printf("%2.2e",new_w1);
			BigDecimal old_gcd_from_rootstack = getOldGCDFromRoot_stack(root_stack);
			//System.out.printf("(%s,%s) old_w1: %2.2e \n",topofrootstack2,topofnodestack2,old_gcd_from_rootstack);
			//		System.out.println("("+topofrootstack2+","+topofnodestack2+") old_w1: "+old_gcd_from_rootstack);
			
			BigDecimal gcd;
			if(new_w1.equals(old_gcd_from_rootstack))
				gcd = new_w1;
			else
				gcd = computeGCD.gcd(new_w1, old_gcd_from_rootstack);
			
		//	System.out.println("Computed gcd: "+gcd);
		//	System.out.printf("Computed gcd: %2.1e \t",gcd);
			if(!gcd.equals(new BigDecimal(1)))
				measured_sup= computeGCD.factorizenew(gcd);
			else 
				measured_sup = 0;
			//System.out.printf(" :: in check %.0f \n",measured_sup); 
		//	System.out.println("getGCD :("+old_gcd_from_rootstack+","+new_w1 +") in check --> "+measured_sup);	
			
			if(measured_sup>=1)
			{
				return true;
			}
			else
				return false;
		}

			
		

			private BigDecimal getOldGCDFromRoot_stack(Stack<String> root_stack2) 
			{
			
				BigDecimal o_w1= new BigDecimal(1);
				DefaultEdge edge = new DefaultEdge();
			//	double tempw;
				if(root_stack2.size()==1)
				{
					o_w1=new_w1;
					return o_w1;
				}
				else 
				{
				
					for(int i=0;i<root_stack2.size()-1;i++)
					{
						edge = g.getEdge(root_stack2.elementAt(i), root_stack2.elementAt(i+1));
						BigDecimal tempw1 = g.getEdgeWeight1big(edge); 
						    if(i==0)
							{
								o_w1 = tempw1;
							//	return o_w1;
							}
						    if(!o_w1.equals(tempw1))
							o_w1=computeGCD.gcd(o_w1, tempw1);
						
					}
					return o_w1;
				}	
		
		
			}

			private boolean checkTopOfStacks() {
				
				if(root_stack.peek().equals(nodes_stack.peek()))
					return true;
				else
					return false;
			}

			private List<String> searchListForTopOnNodeStack(String topofnodestack2) {
				
				List<String> list2 = new ArrayList();	
				int index = searchIndex(topofnodestack2);
				list2 = dcn2[index];
				return list2;
			}

			
			public int searchIndex(String s)
			{
				int i=-1;
				for(int k=0; k < dcn2.length; k++)
				{
					if(s.equals(dcn2[k].get(0).toString()))
					{
						return k;
					}
				}
				
				return i;		
			}

			public String ListToString(List<String> s)
			{
				StringBuilder itemset= new StringBuilder("");
				for(Object item:s)
				{
					itemset.append(item);
				}
				return itemset.toString();
			}
		
		
	}
	
	
	
	//combiner to get the maximum count from map tasks
	
	public static class FIMCombiner implements Reducer<Text, LongWritable, Text, LongWritable>
	{

		@Override
		public void configure(JobConf arg0) {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void close() throws IOException {
			// TODO Auto-generated method stub
			
		}

		@Override
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {
			
	//		System.out.printf("\n\n\n");
	//		System.out.println("In combiner phase");
			
			long maxValue = Long.MIN_VALUE;
			while(values.hasNext())
			{
				maxValue = Math.max(maxValue,values.next().get());
			}
/*
			System.out.printf("\n\n\n");
			
			System.out.println("For "+key+" count: "+maxValue);
			
			System.out.printf("\n\n\n");
*/
			output.collect(key, new LongWritable(maxValue));
			
		}

	}
	
	
	public static class FimReducer extends MapReduceBase implements Reducer<Text, LongWritable, Text, LongWritable>
	{

		
		public void reduce(Text key, Iterator<LongWritable> values,
				OutputCollector<Text, LongWritable> output, Reporter reporter)
				throws IOException {

	//		System.out.println();
	//		System.out.println("in reduce phase");
			
			int sum=0;
			 while (values.hasNext()) 
			 {
	//			 System.out.println();
			     sum += values.next().get();
	//		     System.out.println(sum+" for "+key);
			 }
			 
			 if(sum>=support)
				 output.collect(key, new LongWritable(sum));
			
		}
		
	}
	
	
	
	
	public static void main(String s[]) throws URISyntaxException
	{
		JobClient client = new JobClient();
	    JobConf conf = new JobConf(Main.class);

	    conf.setJobName("ParallelFim");
	    
	  
	    
	    conf.setOutputKeyClass(Text.class);
	    conf.setOutputValueClass(LongWritable.class);

	    DistributedCache.addCacheFile(new URI(LOCAL_PRIME_LIST), conf);
	    
	    FileInputFormat.addInputPath(conf, new Path("input2"));
	    FileOutputFormat.setOutputPath(conf, new Path("output2_4"));
	    
	    conf.setMapperClass(FimMapper.class);
	    conf.setCombinerClass(FIMCombiner.class);
	    conf.setReducerClass(FimReducer.class);
	
	  
	 //   conf.setNumMapTasks(1);
	    client.setConf(conf);

	    try {
	      JobClient.runJob(conf);
	    } catch (Exception e) {
	      e.printStackTrace();
	    }


	}
}
