package ro.unibuc.proiect;

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
import java.util.concurrent.*;

public class MainParalel {

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String inputDirPath = "date_intrare"; 
        String outputFilePath = "rezultat_paralel.txt";
        
        File folder = new File(inputDirPath);
        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".pdf"));

        if (files == null || files.length == 0) {
            System.out.println("Eroare: Nu s-au gasit fisiere PDF!");
            return;
        }

        // Utilizam ConcurrentHashMap conform cerintei din proiect
        Map<String, Integer> globalWordCount = new ConcurrentHashMap<>();
        StringBuilder raportIndividual = new StringBuilder();

        // Gestionarea thread-urilor prin ExecutorService
        int nrFire = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(nrFire);
        List<Future<String>> rezultateViitoare = new ArrayList<>();

        System.out.println("Incepere procesare PARALELA pe " + nrFire + " fire...");
        long startTime = System.currentTimeMillis();

        for (File file : files) {
            // Fiecare fisier este procesat pe un thread separat folosind Callable
            Callable<String> task = () -> {
                StringBuilder sb = new StringBuilder();
                int totalCuvinteFisier = 0;
                Map<String, Integer> localMap = new HashMap<>();

                try {
                    PdfReader reader = new PdfReader(file.getAbsolutePath());
                    for (int i = 1; i <= reader.getNumberOfPages(); i++) {
                        String content = PdfTextExtractor.getTextFromPage(reader, i);
                        String[] words = content.toLowerCase().split("[^a-zA-Z\\d]+");
                        for (String word : words) {
                            if (word.length() > 2) {
                                localMap.put(word, localMap.getOrDefault(word, 0) + 1);
                                totalCuvinteFisier++;
                                // Agregare thread-safe in harta globala
                                globalWordCount.merge(word, 1, Integer::sum);
                            }
                        }
                    }
                    reader.close();

                    sb.append("\n- Fisier: ").append(file.getName()).append(" ---\n");
                    sb.append("Total cuvinte gasite: ").append(totalCuvinteFisier).append("\n");
                    sb.append("Cuvinte unice in fisier: ").append(localMap.size()).append("\n");
                    sb.append("----------------------------\n");
                    // Adaugam frecventele individuale in raport
                    localMap.forEach((w, c) -> sb.append(w).append(": ").append(c).append("\n"));

                } catch (IOException e) {
                    return "Eroare la fisierul " + file.getName();
                }
                return sb.toString();
            };
            rezultateViitoare.add(executor.submit(task));
        }

        // Colectam rezultatele prin Future
        for (Future<String> f : rezultateViitoare) {
            raportIndividual.append(f.get());
        }

        executor.shutdown();
        long endTime = System.currentTimeMillis();
        long durata = endTime - startTime;

        salveazaRaport(globalWordCount, raportIndividual.toString(), outputFilePath, durata);
        System.out.println("Procesare paralela terminata in: " + durata + " ms");
    }

    private static void salveazaRaport(Map<String, Integer> data, String detalii, String path, long time) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            writer.write("=== RAPORT FINAL EXECUTIE PARALELA ===\n");
            writer.write("Timp total: " + time + " ms\n");
            writer.write("Total cuvinte procesate: " + data.values().stream().mapToInt(i -> i).sum() + "\n");
            writer.write("Total cuvinte unice (global): " + data.size() + "\n");
            writer.write("======================================\n");
            writer.write(detalii);
            writer.write("\n\n--- FRECVENTA TOTALA (SORTATA ALFABETIC):\n");
            new TreeMap<>(data).forEach((w, c) -> {
                try { writer.write(w + " : " + c + "\n"); } catch (IOException e) {}
            });
        } catch (IOException e) { e.printStackTrace(); }
    }
}