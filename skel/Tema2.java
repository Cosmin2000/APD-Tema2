
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;


import static java.lang.Integer.parseInt;

public class Tema2 {
    public static void main(String[] args) {
        if (args.length < 3) {
            System.err.println("Usage: Tema2 <workers> <in_file> <out_file>");
            return;
        }
        AtomicInteger inQueue = new AtomicInteger(0);
        ExecutorService tpe = Executors.newFixedThreadPool(parseInt(args[0]));

        // Map care are ca valoare numele fisierul iar ca valoare lista de rezultate
        // pentru acel fisier
        Map<String , List<Pair<Map<Integer, Long>,List<String>>>> mapResults = new HashMap<>();

        // Lista de task-uri Map
        List<MapWorker> mapTasks = new ArrayList<>();


        int dim_frag;
        int nr_files;

        // lista de documente
        Map<String,Document> docs = new HashMap<>();
        List<String> files = new ArrayList<>();
        // citesc fisierul de intrare
        try (BufferedReader scan = new BufferedReader(new FileReader(new File(args[1])))) {
            String line;
            dim_frag = parseInt(scan.readLine());
            nr_files = parseInt(scan.readLine());
            // pozitia in care l-am primit din fisierul de intrare
            // folosit pentru ordonarea fisierelor
            int inputOrder = 0;
            while ((line = scan.readLine()) != null) {
                files.add(line);
                Document doc = new Document(line,inputOrder++);
                docs.put(line,doc);
            }

            // pentru fiecare fisier calculez offset-ul, dimensiunea si construiesc task-uri
            for (String file : files) {

                File f = new File(file);
                int offset = 0;
                int dim = 0;
                mapResults.put(file,new ArrayList<>());
                // calculez offset-ul si dimensiunea pentru fiecare fragment
                while (offset + dim <= f.length()) {
                    offset += dim;
                    // daca e ultimul fragment
                    if (offset + dim_frag > f.length()) {
                        dim = (int)f.length() - offset;
                        inQueue.incrementAndGet();
                        mapTasks.add(new MapWorker(file,offset,dim,inQueue,tpe));
                        break;
                    } else {
                        dim = dim_frag;
                    }
                    // adauga task-ul in lista de task-uri
                    inQueue.incrementAndGet();
                    mapTasks.add(new MapWorker(file,offset,dim,inQueue,tpe));

                }

            }
            // Dau drumul la task-uri de tip Map
            for (MapWorker task : mapTasks) {
                Triplet<Map<Integer, Long>,List<String>, String> res = tpe.submit(task).get();
                Pair<Map<Integer, Long>,List<String>> fragmentResult = new Pair<>(res.first, res.second);
                mapResults.get(res.third).add(fragmentResult);
            }


            tpe.awaitTermination(1, TimeUnit.SECONDS);

            // PARTEA DE REDUCE
            tpe = Executors.newFixedThreadPool(Integer.parseInt(args[0]));

            // Lista de rezultate Reduce
            List<Future<List<Object>>> reduceResultFuture = new ArrayList<>();

            // Pentru fiecare fisier, iau rezultatele de tip Map si pornesc task-urile Reduce
            for (Map.Entry<String,List<Pair<Map<Integer, Long>,List<String>>>> set : mapResults.entrySet()) {
                inQueue.incrementAndGet();
                // fileResults este lista de perechi de rezultate pentru intreg fisierul.
                List<Pair<Map<Integer, Long>, List<String>>> fileResults = new ArrayList<>(set.getValue());
                reduceResultFuture.add(tpe.submit(new ReduceWorker(set.getKey(), fileResults, tpe, inQueue)));

            }
            tpe.awaitTermination(1, TimeUnit.SECONDS);

            // Pentru fiecare rezultatul Reduce al fiecaru fisier, completez rezultatele fisierului
            for (Future<List<Object>> res : reduceResultFuture) {
                List<Object> list = res.get();
                docs.get((String) list.get(0)).rang = (float) list.get(1);
                docs.get((String) list.get(0)).max = (int) list.get(2);
                docs.get((String) list.get(0)).nr_words = (int) list.get(3);

            }

            // Creez o lista de documente din Map-ul de documente.
            List<Document>  documents = new ArrayList<>(docs.values());
            // Sortez fisierele (dupa rang, apoi dupa ordinea din fisierul de intrare)
            Collections.sort(documents);


            // Scriu in fisier
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
