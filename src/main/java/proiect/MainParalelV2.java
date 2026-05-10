package proiect;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveTask;

public class MainParalelV2 {

    // Daca un task are mai mult de 3 pagini, se va divide in sub-task-uri mai mici
    private static final int THRESHOLD = 3; 

    //Extinde RecursiveTask deoarece returneaza un rezultat (Map-ul cu frecventa cuvintelor)
    static class PageTextTask extends RecursiveTask<Map<String, Integer>> {

        private static final long serialVersionUID = 1L;
        private final String[] pageTexts;
        private final int from;
        private final int to;

        PageTextTask(String[] pageTexts, int from, int to) {
            this.pageTexts = pageTexts;
            this.from = from;
            this.to = to;
        }

        @Override
        protected Map<String, Integer> compute() {
            int rangeSize = to - from + 1;

            // Daca intervalul este sub prag, procesam paginile direct
            if (rangeSize <= THRESHOLD) {
                return numaraCuvinte(from, to);
            }

            // Impartim intervalul curent in doua jumatati
            int mid = from + rangeSize / 2 - 1;
            PageTextTask stanga  = new PageTextTask(pageTexts, from, mid);
            PageTextTask dreapta = new PageTextTask(pageTexts, mid + 1, to);

            // Executam task-ul din stanga in fundal (asincron)
            stanga.fork(); 
            
            // Procesam task-ul din dreapta in thread-ul curent pentru eficienta
            Map<String, Integer> rezDreapta = dreapta.compute(); 
            
            // Asteptam finalizarea task-ului din stanga si preluam rezultatul
            Map<String, Integer> rezStanga  = stanga.join();     

            // Combinam rezultatele din cele doua ramuri
            rezStanga.forEach((k, v) -> rezDreapta.merge(k, v, Integer::sum));
            return rezDreapta;
        }

        //Metoda care realizeaza numararea efectiva a cuvintelor pe un segment de pagini
        private Map<String, Integer> numaraCuvinte(int startIdx, int endIdx) {
            Map<String, Integer> wc = new HashMap<>();
            for (int i = startIdx; i <= endIdx; i++) {
                // Transformarea textului in litere mici si eliminarea semnelor de punctuatie
                String[] words = pageTexts[i].toLowerCase().split("[^a-zA-Z\\d]+");
                for (String word : words) {
                    if (word.length() > 2) { // Ignoram cuvintele foarte scurte
                        wc.merge(word, 1, Integer::sum);
                    }
                }
            }
            return wc;
        }
    }

    public static void main(String[] args) throws Exception {
    	// Configurarea cailor pentru directoarele de intrare si fisierul de iesire
        String inputDirPath   = "date_intrare";
        String outputFilePath = "rezultat_paralel_v2.txt";

     // Identificarea fisierelor PDF din directorul specificat
        File folder = new File(inputDirPath);
        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".pdf"));

        if (files == null || files.length == 0) {
            System.out.println("Eroare: Nu s-au gasit fisiere PDF!");
            return;
        }

        // Initializam Pool-ul cu un numar de thread-uri egal cu nucleele procesorului
        int nrNuclee = Runtime.getRuntime().availableProcessors();
        ForkJoinPool forkJoinPool = new ForkJoinPool(nrNuclee);

        System.out.println("Incepere procesare paralela (ForkJoinPool, " + nrNuclee + " nuclee)...");
        long startTime = System.currentTimeMillis();

        Map<String, Integer> globalWordCount = new HashMap<>(); // Pentru statistici agregate pe toate fisierele
        StringBuilder raportIndividual = new StringBuilder();   // Pentru detaliile fiecarui fisier

        try {
            for (File file : files) {
                System.out.println("Procesez: " + file.getName());

                // Citirea I/O si extragerea textului, lasata secventiala pentru a evita supraincarcarea memoriei la fisiere mari
                String[] pageTexts;
                int numPages;
                try {
                    PdfReader reader = new PdfReader(file.getAbsolutePath());
                    numPages  = reader.getNumberOfPages();
                    pageTexts = new String[numPages];
                    for (int i = 0; i < numPages; i++) {
                        pageTexts[i] = PdfTextExtractor.getTextFromPage(reader, i + 1);
                    }
                    reader.close();
                } catch (IOException e) {
                    System.err.println("Nu pot citi: " + file.getName());
                    continue;
                }

                // Lansarea task-ului radacina in ForkJoinPool
                // Toate nucleele vor lucra simultan pentru a numara cuvintele din fisierul curent
                PageTextTask rootTask = new PageTextTask(pageTexts, 0, numPages - 1);
                Map<String, Integer> fileWordCount = forkJoinPool.invoke(rootTask);

                // Calculam statistici per fisier
                int totalFisier = fileWordCount.values().stream().mapToInt(i -> i).sum();

                raportIndividual.append("\n- Fisier: ").append(file.getName()).append(" ---\n");
                raportIndividual.append("Numar pagini: ").append(numPages).append("\n");
                raportIndividual.append("Total cuvinte gasite: ").append(totalFisier).append("\n");
                raportIndividual.append("Cuvinte unice in fisier: ").append(fileWordCount.size()).append("\n");
                
                // Sortam cuvintele alfabetic doar pentru raportul individual
                new TreeMap<>(fileWordCount).forEach((w, c) -> raportIndividual.append(w).append(": ").append(c).append("\n"));

                // Adaugam datele fișierului curent in numaratoarea globala
                fileWordCount.forEach((k, v) -> globalWordCount.merge(k, v, Integer::sum));
            }
        } finally {
            forkJoinPool.shutdown(); // Inchidem pool-ul de thread-uri la final
        }

        long durata = System.currentTimeMillis() - startTime;

        // Scriem rezultatele finale in fisierul de iesire
        salveazaRaport(globalWordCount, raportIndividual.toString(), outputFilePath, durata, nrNuclee);
        
        System.out.println("Procesare paralela V2 terminata in: " + durata + " ms");
        System.out.println("Rezultate salvate in: " + outputFilePath);
    }

    //Metoda pentru generarea raportului final folosind BufferedWriter
    private static void salveazaRaport(Map<String, Integer> data, String detalii, String path, long time, int nrNuclee) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            writer.write("== RAPORT FINAL EXECUTIE PARALELA 2 ==\n");
            writer.write("Mod executie: PARALEL - ForkJoinPool\n");
            writer.write("Nuclee folosite: " + nrNuclee + "\n");
            writer.write("Timp total: " + time + " ms\n");
            writer.write("Total cuvinte procesate: " + data.values().stream().mapToInt(i -> i).sum() + "\n");
            writer.write("Total cuvinte unice (global): " + data.size() + "\n");
            writer.write("\n- DETALII PER FISIER:");
            writer.write(detalii);
            writer.write("\n\n- FRECVENTA TOTALA (SORTATA ALFABETIC):\n");
            // Folosim TreeMap pentru a asigura sortarea alfabetica in raportul global
            new TreeMap<>(data).forEach((w, c) -> {
                try { 
                		writer.write(w + " : " + c + "\n"); 
                	} 
                catch (IOException e) { 
                	e.printStackTrace(); 
                	}
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}