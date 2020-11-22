package com.opstty;

import com.opstty.job.*;
import org.apache.hadoop.util.ProgramDriver;

public class AppDriver {
    public static void main(String argv[]) {
        int exitCode = -1;
        ProgramDriver programDriver = new ProgramDriver();

        try {
            programDriver.addClass("max", SumTrees.class,
                    "A map/reduce program that shows the district containing the most trees.");

            programDriver.addClass("sum", SumTrees.class,
                    "A map/reduce program that shows the district containing the most trees.");

            programDriver.addClass("oldest", OldestTree.class,
                    "A map/reduce program that shows the oldest tree and its district.");

            programDriver.addClass("sortH", SortHeight.class,
                    "A map/reduce program that shows the height for each species of tree in Paris sorted from the smallest to the largest.");

            programDriver.addClass("maxH", MaxHeight.class,
                    "A map/reduce program that shows the max height for each species of tree in Paris.");

            programDriver.addClass("nbTrees", NbTreesBySpecies.class,
                    "A map/reduce program that counts the number of trees by species in Paris.");

            programDriver.addClass("species", Species.class,
                    "A map/reduce program that counts the number of species of tree in Paris.");

            programDriver.addClass("district", District.class,
                    "A map/reduce program that counts the districts contaning trees.");

            programDriver.addClass("wordcount", WordCount.class,
                    "A map/reduce program that counts the words in the input files.");


            exitCode = programDriver.run(argv);
        } catch (Throwable throwable) {
            throwable.printStackTrace();
        }

        System.exit(exitCode);
    }
}
