package MapRedClasses;

import java.io.*;


public class InputFileCreator {

	public static final String INPUTLOC = "/home/user/hadoop-1.0.4/projects/ParallelFim/input5/data2.dat";
	public static final String OUTPUTDIR = "/home/user/hadoop-1.0.4/projects/ParallelFim/input5/";
	
	public static void main(String s[])
	{
		BufferedWriter brout;
		try {
			
			BufferedReader brin = new BufferedReader(new FileReader(new File(INPUTLOC)));
			
			
			String linein = null;
			StringBuilder lineout=new StringBuilder();
			
			int j=0;
			
			int i=0;
			while((linein = brin.readLine())!=null)
			{
				lineout.append(linein);
				lineout.append(";");
				i++;

				if(i>99)
				{
					brout = new BufferedWriter(new FileWriter(new File(OUTPUTDIR+"formateddata"+j+".dat")));
					j++;
					brout.write(lineout.toString());
					lineout = null;
					lineout = new StringBuilder();
					i=0;
					brout.close();
					
				}
				
			}
			brout = new BufferedWriter(new FileWriter(new File(OUTPUTDIR+"formateddata"+j+".dat")));
			brout.write(lineout.toString());
			brout.close();
			brin.close();
			
			
		} catch (FileNotFoundException e) {
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		catch(IOException e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
		
		
	}
}
