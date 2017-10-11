package com.reader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class DataReader {
	
	public void read() {
		
		try {
			BufferedReader br = new BufferedReader(new FileReader("src/allData.txt"));
			for(int i=0;i<10;i++){
				String s = br.readLine();
				System.out.println(s);
				String[] str = s.split("\t");
				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSSSSS+00:00");
				Date date = simpleDateFormat.parse(str[0]);
				System.out.println(date.getTime());
			}
			br.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		/*DataReader dr = new DataReader();
		dr.read();*/
		
		Random random = new Random();
		
		for(int i=0;i<10;i++){
			System.out.print(random.nextInt(2) + " ");
		}

	}

}
