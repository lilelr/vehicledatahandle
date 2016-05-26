package MyTest;

import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

/**
 * Created by yuxiao on 16/5/26.
 */
public class AlarmStatusTest {

    @Test
    public void alrmTest(){
        try{
            File dataFile = new File("/Users/yuxiao/project/ExperimentRoom/note/32000.csv");
            BufferedReader bufferedReader = new BufferedReader(new FileReader(dataFile));
            String line = null;
            int[] res = new int[32];
            while ((line = bufferedReader.readLine()) != null) {
                String[] lss = line.split(",");
                if (lss.length >= 18) {
                    int alarmField = Integer.valueOf(lss[17]);
                    int bitIndex =0;
                    while (alarmField!=0){
                        if((alarmField&1) == 1){
                            res[bitIndex]++;
                        }
                        alarmField >>= 1;
                        bitIndex++;
                    }
                }
            }
            bufferedReader.close();

            for(int i=0;i<32;i++){
                if(res[i]!=0){
                    System.out.println(i+":"+res[i]);
                }
            }
        } catch (Exception e){
            e.printStackTrace();
        }

    }
}
