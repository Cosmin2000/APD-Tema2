import javax.print.Doc;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static java.lang.Integer.parseInt;

public class Tema2 {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }
        AtomicInteger inQueue = new AtomicInteger(0);
        ExecutorService tpe = Executors.newFixedThreadPool(parseInt(args[0]));
        Map<String , List<Future<List<Object>>>> results = new HashMap<>();
        //List<Future<List<Object>>> res = new ArrayList<>();


        int dim_frag;
        int nr_files;
        Map<String,Document> docs = new HashMap<>();
        List<String> files = new ArrayList<>();
        try (BufferedReader scan = new BufferedReader(new FileReader(new File(args[1])))) {
            String line;
            dim_frag = parseInt(scan.readLine());
            nr_files = parseInt(scan.readLine());
            int order = 0;
            while ((line = scan.readLine()) != null) {
                files.add(line);
                Document doc = new Document(line,order++);
                docs.put(line,doc);

            }

            // pentru fiecare fisier calculez offset-ul, dimensiunea si pornesc thread-urile map
            for (String file : files) {

                List<Future<List<Object>>> res = new ArrayList<>();
                File f = new File(file);
                int size = (int)f.length();
                int offset = 0;
                int dim = 0;
                while (offset + dim <= f.length()) {
                    offset += dim;
                    if (offset + dim_frag > f.length()) {
                        dim = (int)f.length() - offset;
                        inQueue.incrementAndGet();
                        res.add(tpe.submit(new Worker(file,offset,dim,inQueue,tpe)));
                        break;
                    } else {
                        dim = dim_frag;
                    }
                    inQueue.incrementAndGet();
                   res.add(tpe.submit(new Worker(file,offset,dim,inQueue,tpe)));

                }
                results.put(file,res);
            }
            tpe.awaitTermination(10, TimeUnit.SECONDS);

            tpe = Executors.newFixedThreadPool(Integer.parseInt(args[0]));

            List<Future<Map<String, List<Object>>>> reduceResultFuture = new ArrayList<>();
            List<Map<String, List<Object>>> reduceResult = new ArrayList<>();


            for (Map.Entry<String,List<Future<List<Object>>>> set : results.entrySet()) {

                // dictionares este o lista de rezultate (lista) de la MAP
                inQueue.incrementAndGet();
                List<List<Object>> dictionares = new ArrayList<>();
                for (Future<List<Object>> t : set.getValue()) {
                    List<Object> objects = t.get();
                    dictionares.add(objects);
                }
                reduceResultFuture.add(tpe.submit(new Reduce(set.getKey(), dictionares,tpe,inQueue)));

            }
            tpe.awaitTermination(10, TimeUnit.SECONDS);

            for (Future<Map<String, List<Object>>> res : reduceResultFuture) {
                Map<String, List<Object>> mp = res.get();
                for (String str : mp.keySet()) {
                    docs.get(str).rang = (float) mp.get(str).get(0);
                    docs.get(str).max = (int) mp.get(str).get(1);
                    docs.get(str).nr_words = (int) mp.get(str).get(2);
                }
            }
            List<Document>  documents = new ArrayList<>(docs.values());
            Collections.sort(documents);

            BufferedWriter writer = new BufferedWriter(new FileWriter(args[2]));
            for (Document doc : documents) {
                writer.write(doc + "\n");
            }

            writer.close();
        } catch (IOException | InterruptedException | ExecutionException e) {
            e.printStackTrace();
        }

    }
}
