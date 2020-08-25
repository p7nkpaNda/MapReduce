import javafx.util.Pair;
import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

class splittingThread extends Thread{
    String content;
    public ArrayList<String> words;
    splittingThread(String content){
        this.content = content;
    }
    @Override
    public void run(){
        words = MapReduce.splitting(content);
    }
}

class MappingThread extends Thread{
    ArrayList<String> words;
    public ArrayList<Pair<String, Integer>> mapping;
    MappingThread(ArrayList<String> words){
        this.words = words;
    }
    @Override
    public void run(){
        mapping = MapReduce.Mapping(words);
    }
}

class ShufflingThread extends Thread{
    ArrayList<Pair<String, Integer>> mapping1;
    ArrayList<Pair<String, Integer>> mapping2;
    ArrayList<Pair<String, Integer>> mapping3;
    char start ;
    char end ;
    public HashMap<String, ArrayList<Integer>> shuffling;
    ShufflingThread(ArrayList<Pair<String, Integer>> mapping1,
                    ArrayList<Pair<String, Integer>> mapping2,
                    ArrayList<Pair<String, Integer>> mapping3,
                    char start, char end){
        this.mapping1 = mapping1;
        this.mapping2 = mapping2;
        this.mapping3 = mapping3;
        this.start = start;
        this.end = end;
    }
    @Override
    public void run(){
        ArrayList<Pair<String, Integer>> mapping=new ArrayList<>();
        for (Pair<String, Integer> pair:mapping1){
            char key = pair.getKey().charAt(0);
            if (key>=start && key<=end){
                mapping.add(pair);
            }
        }
        for (Pair<String, Integer> pair:mapping2){
            char key = pair.getKey().charAt(0);
            if (key>=start && key<=end){
                mapping.add(pair);
            }
        }
        for (Pair<String, Integer> pair:mapping3){
            char key = pair.getKey().charAt(0);
            if (key>=start && key<=end){
                mapping.add(pair);
            }
        }
        shuffling = MapReduce.Shuffling(mapping);
    }
}

class ReducingThread extends Thread{
    HashMap<String, ArrayList<Integer>> shuffling;
    public HashMap<String, Integer> result;
    ReducingThread(HashMap<String, ArrayList<Integer>> shuffling){
        this.shuffling= shuffling;
    }
    @Override
    public void run(){
        result = MapReduce.Reducing(shuffling);
    }
}

public class MapReduce {
    static ArrayList<String> splitting(String content){
        ArrayList<String> words = new ArrayList<>();
        StringTokenizer st = new StringTokenizer(content, ",.!:;[]--|?' \n\r");
        while (st.hasMoreTokens()) {
            String word = st.nextToken();
            word = word.toLowerCase();
            words.add(word);
        }
        return words;
    }

    static ArrayList<Pair<String, Integer>> Mapping(ArrayList<String> words){
        ArrayList<Pair<String, Integer>> mapping = new ArrayList<>();
        for(String word:words){
            mapping.add(new Pair<String, Integer>(word,1));
        }
        return mapping;
    }

    static HashMap<String, ArrayList<Integer>> Shuffling(ArrayList<Pair<String, Integer>> mapping){
        HashMap<String, ArrayList<Integer>> shuffling = new HashMap<>();
        for (Pair<String, Integer> pair:mapping){
            String key = pair.getKey();
            if (shuffling.containsKey(key)){
                shuffling.get(key).add(1);
            }else{
                ArrayList<Integer> list = new ArrayList<>();
                list.add(1);
                shuffling.put(key,list);
            }
        }
        return shuffling;
    }

    static HashMap<String, Integer> Reducing(HashMap<String, ArrayList<Integer>> shuffling){
        HashMap<String, Integer> result = new HashMap<>();
        for (String word : shuffling.keySet()) {
            result.put(word, shuffling.get(word).size());
        }
        return result;
    }

    static String Readallfile(String filepath){
        File file = new File(filepath);
        Long fileLengthLong = file.length();
        byte[] fileContent = new byte[fileLengthLong.intValue()];
        try {
            FileInputStream inputStream = new FileInputStream(file);
            inputStream.read(fileContent);
            inputStream.close();
        } catch (Exception e) {
            System.out.println("读取文件失败");
            e.printStackTrace();
        }
        String content = new String(fileContent);
        return content;
    }

    static void Outfile(HashMap<String, Integer> result,String filepath){
        try {
            BufferedWriter bw = new BufferedWriter(new FileWriter(filepath)) ;
            for(String key : result.keySet()){
                String s = key + ": " + result.get(key) + "\n";
                bw.write(s);
            }
            bw.close();
        } catch (IOException e) {
            System.out.println("写入文件失败");
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //单线程
        long startTime=System.currentTimeMillis();
        String content = Readallfile("./hamlet.txt");
        ArrayList<String> words = splitting(content);
        ArrayList<Pair<String, Integer>> mapping = Mapping(words);
        HashMap<String, ArrayList<Integer>> shuffling = Shuffling(mapping);
        HashMap<String, Integer> result = Reducing(shuffling);
        Outfile(result,"res-single.txt");
        long endTime=System.currentTimeMillis();
        System.out.println("单线程程序运行时间： "+(endTime-startTime)+"ms");

        //多线程
        startTime=System.currentTimeMillis();
        content = Readallfile("./hamlet.txt");
        splittingThread thread1 = new splittingThread(content.substring(0,2000));
        splittingThread thread2 = new splittingThread(content.substring(2000,4000));
        splittingThread thread3 = new splittingThread(content.substring(4000));
        thread1.start();
        thread2.start();
        thread3.start();
        try{
            thread1.join();
            thread2.join();
            thread3.join();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        ArrayList<String> words1 = thread1.words;
        ArrayList<String> words2 = thread2.words;
        ArrayList<String> words3 = thread3.words;

        MappingThread thread11 = new MappingThread(words1);
        MappingThread thread12 = new MappingThread(words2);
        MappingThread thread13 = new MappingThread(words3);
        thread11.start();
        thread12.start();
        thread13.start();
        try{
            thread11.join();
            thread12.join();
            thread13.join();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        ArrayList<Pair<String, Integer>> mapping1 = thread11.mapping;
        ArrayList<Pair<String, Integer>> mapping2 = thread12.mapping;
        ArrayList<Pair<String, Integer>> mapping3 = thread13.mapping;

        ShufflingThread thread111 = new ShufflingThread(mapping1,mapping2,mapping3,'a','g');
        ShufflingThread thread112 = new ShufflingThread(mapping1,mapping2,mapping3,'h','n');
        ShufflingThread thread113 = new ShufflingThread(mapping1,mapping2,mapping3,'o','t');
        ShufflingThread thread114 = new ShufflingThread(mapping1,mapping2,mapping3,'u','z');
        thread111.start();
        thread112.start();
        thread113.start();
        thread114.start();
        try{
            thread111.join();
            thread112.join();
            thread113.join();
            thread114.join();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        HashMap<String, ArrayList<Integer>> shuffling1 = thread111.shuffling;
        HashMap<String, ArrayList<Integer>> shuffling2 = thread112.shuffling;
        HashMap<String, ArrayList<Integer>> shuffling3 = thread113.shuffling;
        HashMap<String, ArrayList<Integer>> shuffling4 = thread114.shuffling;

        ReducingThread thread1111 = new ReducingThread(shuffling1);
        ReducingThread thread1112 = new ReducingThread(shuffling2);
        ReducingThread thread1113 = new ReducingThread(shuffling3);
        ReducingThread thread1114 = new ReducingThread(shuffling4);
        thread1111.start();
        thread1112.start();
        thread1113.start();
        thread1114.start();
        try{
            thread1111.join();
            thread1112.join();
            thread1113.join();
            thread1114.join();
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        HashMap<String, Integer> result1 = thread1111.result;
        HashMap<String, Integer> result2 = thread1112.result;
        HashMap<String, Integer> result3 = thread1113.result;
        HashMap<String, Integer> result4 = thread1114.result;

        result.clear();
        result.putAll(result1);
        result.putAll(result2);
        result.putAll(result3);
        result.putAll(result4);
        Outfile(result,"res-thread.txt");
        endTime=System.currentTimeMillis();
        System.out.println("多线程程序运行时间： "+(endTime-startTime)+"ms");
    }
}
