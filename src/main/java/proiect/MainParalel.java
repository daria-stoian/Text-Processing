package proiect;
import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class MainParalel {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        // Configurarea cailor pentru directoarele de intrare si fisierul de iesire
        String inputDirPath   = "date_intrare";
        String outputFilePath = "rezultat_paralel.txt";

        // Identificarea fisierelor PDF din directorul specificat
        File folder = new File(inputDirPath);
        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".pdf"));

        // Verificarea existentei fisierelor de procesat
        if (files == null || files.length == 0) {
            System.out.println("Eroare: Nu s-au gasit fisiere PDF!");
            return;
        }

        // Determinarea numarului de nuclee disponibile pentru a crea un pool de thread-uri optim
        int nrNuclee = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(nrNuclee);

        System.out.println("Incepere procesare paralela (ExecutorService, " + nrNuclee + " nuclee)...");
        long startTime = System.currentTimeMillis();

        // Structuri pentru stocarea rezultatelor globale si a detaliilor pentru raport
        Map<String, Integer> globalWordCount = new HashMap<>();
        StringBuilder raportIndividual = new StringBuilder();

        try {
            // Bucla pentru procesarea fiecarui fisier identificat
            for (File file : files) {
                System.out.println("Procesez: " + file.getName());

                // Extragerea textului din toate paginile PDF-ului
                // Se face secvential pentru a evita erorile de citire concurenta din același flux I/O
                String[] pageTexts;
                int numPages;
                try {
                    PdfReader reader = new PdfReader(file.getAbsolutePath());
                    numPages  = reader.getNumberOfPages();
                    pageTexts = new String[numPages];
                    for (int i = 0; i < numPages; i++) {
                        // Extragerea textului pagina cu pagina intr-un array de String-uri
                        pageTexts[i] = PdfTextExtractor.getTextFromPage(reader, i + 1);
                    }
                    reader.close();
                } catch (IOException e) {
                    System.err.println("Nu pot citi: " + file.getName());
                    continue;
                }

                // Impartirea paginilor in segmente fixe
                // Calculam cate pagini revin fiecarui thread
                int chunkSize = Math.max(1, (int) Math.ceil((double) numPages / nrNuclee));
                List<Future<Map<String, Integer>>> futures = new ArrayList<>();

                for (int start = 0; start < numPages; start += chunkSize) {
                    final int from = start;
                    final int to   = Math.min(start + chunkSize - 1, numPages - 1);

                    // Definirea sarcinii pentru fiecare segment de pagini
                    Callable<Map<String, Integer>> task = () -> {
                        Map<String, Integer> wc = new HashMap<>();
                        for (int i = from; i <= to; i++) {
                            // Tokenizarea textului si numararea cuvintelor (ignorand caracterele non-alfanumerice)
                            for (String word : pageTexts[i].toLowerCase().split("[^a-zA-Z\\d]+")) {
                                if (word.length() > 2) { // Filtrare: cuvinte cu lungime > 2
                                    wc.merge(word, 1, Integer::sum);
                                }
                            }
                        }
                        return wc; // Returneaza harta de frecventa pentru chunk-ul curent
                    };
                    // Trimiterea sarcinii catre executor si stocarea obiectului Future
                    futures.add(executor.submit(task));
                }

                // Colectare rezultate: Combinarea hartilor partiale primite de la fiecare thread
                Map<String, Integer> fileWordCount = new HashMap<>();
                for (Future<Map<String, Integer>> future : futures) {
                    // .get() este blocant si asteapta finalizarea thread-ului respectiv
                    future.get().forEach((k, v) -> fileWordCount.merge(k, v, Integer::sum));
                }

                // Generarea statisticilor pentru fisierul curent
                int totalFisier = fileWordCount.values().stream().mapToInt(i -> i).sum();

                raportIndividual.append("\n- Fisier: ").append(file.getName()).append(" ---\n");
                raportIndividual.append("Numar pagini: ").append(numPages).append("\n");
                raportIndividual.append("Total cuvinte gasite: ").append(totalFisier).append("\n");
                raportIndividual.append("Cuvinte unice in fisier: ").append(fileWordCount.size()).append("\n");
                
                // Adaugarea fiecarui cuvant si a frecventei sale in raportul textual
                fileWordCount.forEach((w, c) -> raportIndividual.append(w).append(": ").append(c).append("\n"));

                // Integrarea rezultatelor fisierului curent in numaratoarea globala
                fileWordCount.forEach((k, v) -> globalWordCount.merge(k, v, Integer::sum));
            }
        } finally {
            // Oprirea pool-ului de thread-uri si asteptarea finalizarii sarcinilor ramase
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.MINUTES);
        }

        // Calcularea duratei totale de executie
        long durata = System.currentTimeMillis() - startTime;

        // Salvarea datelor finale in fisierul raport
        salveazaRaport(globalWordCount, raportIndividual.toString(), outputFilePath, durata, nrNuclee);
        System.out.println("Procesare paralela V1 terminata in: " + durata + " ms");
        System.out.println("Rezultate salvate in: " + outputFilePath);
    }

    // Metoda pentru scrierea rezultatelor agregate intr-un fisier text
    private static void salveazaRaport(Map<String, Integer> data, String detalii, String path, long time, int nrNuclee) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            writer.write("== RAPORT FINAL EXECUTIE PARALELA 1 ==\n");
            writer.write("Mod executie: PARALEL - ExecutorService (Callable/Future)\n");
            writer.write("Nuclee folosite: " + nrNuclee + "\n");
            writer.write("Timp total: " + time + " ms\n");
            writer.write("Total cuvinte procesate: " + data.values().stream().mapToInt(i -> i).sum() + "\n");
            writer.write("Total cuvinte unice (global): " + data.size() + "\n");
            writer.write("\n- DETALII PER FISIER:");
            writer.write(detalii);
            writer.write("\n\n- FRECVENTA TOTALA (SORTATA ALFABETIC):\n");
            
            // Folosim TreeMap pentru a sorta automat cuvintele alfabetic inainte de scriere
            new TreeMap<>(data).forEach((w, c) -> {
                try { writer.write(w + " : " + c + "\n"); } catch (IOException e) { e.printStackTrace(); }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}