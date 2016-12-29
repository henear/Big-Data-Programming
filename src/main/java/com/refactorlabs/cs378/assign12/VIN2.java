package com.refactorlabs.cs378.assign12;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Scanner;
import java.util.TreeMap;

public class VIN2 {

	public static void main(String[] args) throws FileNotFoundException {
		// TODO Auto-generated method stub
		Scanner sc = new Scanner(new File("temp.csv"));
		PrintStream p = new PrintStream("Assignment12OutputEvent.txt");
		TreeMap<String, Integer> tm = new TreeMap<String, Integer>();
		while(sc.hasNextLine()){
			String s = sc.nextLine();
			String VIN = s.split(",")[0];
			String eventtype = (s.split(",")[1]).split(" ")[0];
			String key = VIN + "," + eventtype;			
			if(tm.containsKey(key)){
				int oldCount = tm.get(key);
				oldCount ++;
				tm.put(key, oldCount);
			}else{
				tm.put(key, 1);
			}
		}		
		for(String key: tm.keySet()){
			p.println(key+","+tm.get(key));
		}
		sc.close();
		p.close();
	}
}
