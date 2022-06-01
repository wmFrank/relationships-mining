package task4;

public class Driver {
    private static int times = 30;

    public static void main(String[] args) {
        try {
           String[] ParaforGB = {"", args[1] + "/Data0"};
           ParaforGB[0] = args[0];
           GraphBuilder.main(ParaforGB);

           String[] ParaforPR = {"",""};
           for(int i = 0; i < times; i++){
               ParaforPR[0] = args[1] + "/Data" + Integer.toString(i);
               ParaforPR[1] = args[1] + "/Data" + Integer.toString(i + 1);
               PageRanker.main(ParaforPR);
           }

        } catch (Exception e) { e.printStackTrace(); }
    }
}
