package MapRedClasses;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.TreeSet;

import org.apache.hadoop.io.WritableComparable;

public class SortedItemSet extends TreeSet<String> implements WritableComparable<SortedItemSet> 
{
//	Set<String> s1;
	//Set<String> s2;
	public SortedItemSet() {
		super();
		//s1  = new TreeSet<String>();
	}
	

	@Override
	public void readFields(DataInput in) throws IOException {
		
		String s = in.readLine();
		StringTokenizer stk = new StringTokenizer(s);
		
		while(stk.hasMoreTokens())
			this.add(stk.nextToken());
		
	}

	@Override
	public void write(DataOutput out) throws IOException {
		
		out.writeUTF(this.toString());
	}

	@Override
	public int compareTo(SortedItemSet s2) {
		Iterator<String> i1 = this.iterator();
		Iterator<String> i2 = s2.iterator();
		int temp=2;
		Comparator c =  this.comparator();
		System.out.println("before while");
		while(i1.hasNext() && i2.hasNext())
		{
			System.out.println("in while loop");
			
			String a = i1.next();
			String b = i2.next();
			try{
			 temp=a.compareTo(b);
			}catch(Exception e)
			{
				System.out.println(a+b);
				System.out.println(temp);
				System.out.println(e);
			}
			System.out.println();
			if(temp==0)
				continue;
			else
				break;
		}
		
		return temp;
	}
	public static void main(String s[]) throws Exception{
		
		SortedItemSet o = new SortedItemSet();
	
//		DataInputStream in = new DataInputStream(System.in)
		o.readFields(new DataInputStream(System.in));
		
		SortedItemSet o1 = new SortedItemSet();
		o1.readFields(new DataInputStream(System.in));
		
		System.out.println(o);
		System.out.println(o1.toString());
		
		System.out.println(o.compareTo(o1));
	}

	
}
