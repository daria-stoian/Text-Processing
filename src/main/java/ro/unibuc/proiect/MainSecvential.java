package ro.unibuc.proiect;

import com.itextpdf.text.pdf.PdfReader;
import com.itextpdf.text.pdf.parser.PdfTextExtractor;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

public class MainSecvential {

    public static void main(String[] args) {
        String inputDirPath = "date_intrare"; 
        String outputFilePath = "rezultat.txt";
        
        File folder = new File(inputDirPath);
        File[] files = folder.listFiles((dir, name) -> name.toLowerCase().endsWith(".pdf"));

        if (files == null || files.length == 0) {
            System.out.println("Eroare: Nu s-au gasit fisiere PDF in folderul 'date_intrare'!");
            return;
        }

        Map<String, Integer> globalWordCount = new HashMap<>();
        StringBuilder raportIndividual = new StringBuilder();

        System.out.println("Incepere procesare secventiala...");
        long startTime = System.currentTimeMillis();

        for (File file : files) {
            System.out.println("Se proceseaza: " + file.getName());
            
            // Obtinem frecventa pentru fisierul curent
            Map<String, Integer> fileWordCount = obtineFrecventaFisier(file);
            
            // Calculam statisticile automate pentru acest fisier
            int totalCuvinteFisier = fileWordCount.values().stream().mapToInt(Integer::intValue).sum();
            int cuvinteUniceFisier = fileWordCount.size();
            
            // Construim raportul pentru fisierul curent
            raportIndividual.append("\n- Fisier: ").append(file.getName()).append(" ---\n");
            raportIndividual.append("Total cuvinte gasite: ").append(totalCuvinteFisier).append("\n");
            raportIndividual.append("Cuvinte unice in fisier: ").append(cuvinteUniceFisier).append("\n");
            raportIndividual.append("----------------------------\n");
            
            fileWordCount.forEach((word, count) -> {
                raportIndividual.append(word).append(": ").append(count).append("\n");
                // Actualizam frecventa globala (Suma)
                globalWordCount.merge(word, count, Integer::sum);
            });
        }

        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;

        // Salvam raportul final care include si totalurile globale
        salveazaRaportComplet(globalWordCount, raportIndividual.toString(), outputFilePath, totalTime);

        System.out.println("\nProcesare terminata!");
        System.out.println("Timp total de executie: " + totalTime + " ms");
        System.out.println("Rezultate salvate in: " + outputFilePath);
    }

    private static Map<String, Integer> obtineFrecventaFisier(File file) {
        Map<String, Integer> wordMap = new HashMap<>();
        try {
            PdfReader reader = new PdfReader(file.getAbsolutePath());
            int numPages = reader.getNumberOfPages();
            for (int i = 1; i <= numPages; i++) {
                String pageContent = PdfTextExtractor.getTextFromPage(reader, i);
                // Curatam textul: litere mici si eliminam caractere non-alfanumerice
                String[] words = pageContent.toLowerCase().split("[^a-zA-Z\\d]+");
                for (String word : words) {
                    if (word.length() > 2) { // Ignoram cuvintele sub 3 caractere
                        wordMap.put(word, wordMap.getOrDefault(word, 0) + 1);
                    }
                }
            }
            reader.close();
        } catch (IOException e) {
            System.err.println("Eroare la citirea fisierului " + file.getName() + ": " + e.getMessage());
        }
        return wordMap;
    }

    private static void salveazaRaportComplet(Map<String, Integer> globalData, String detaliiFisiere, String path, long time) {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(path))) {
            // Calculam statisticile globale
            int totalCuvinteGlobal = globalData.values().stream().mapToInt(Integer::intValue).sum();
            int cuvinteUniceGlobal = globalData.size();

            writer.write("=== RAPORT FINAL EXECUTIE ===\n");
            writer.write("Mod executie: SECVENTIAL\n");
            writer.write("Timp total: " + time + " ms\n");
            writer.write("Total cuvinte procesate (toate fisierele): " + totalCuvinteGlobal + "\n");
            writer.write("Total cuvinte unice (global): " + cuvinteUniceGlobal + "\n");
            writer.write("=============================\n");

            writer.write("\n--- DETALII PER FISIER:");
            writer.write(detaliiFisiere);

            writer.write("\n\n--- FRECVENTA TOTALA (TOATE FISIERELE - SORTATE ALFABETIC):\n");
            // TreeMap sorteaza automat cheile (cuvintele) alfabetic
            TreeMap<String, Integer> sortedGlobal = new TreeMap<>(globalData);
            for (Map.Entry<String, Integer> entry : sortedGlobal.entrySet()) {
                writer.write(entry.getKey() + " : " + entry.getValue() + "\n");
            }
        } catch (IOException e) {
            System.err.println("Eroare la scrierea raportului: " + e.getMessage());
        }
    }
}