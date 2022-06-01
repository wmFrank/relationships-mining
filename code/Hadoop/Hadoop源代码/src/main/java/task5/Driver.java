package task5;

public class Driver {
    private static int times = 15;

    public static void main(String[] args) {
        try {
           String[] ParaforGB = {"", args[1] + "/Data0"};
           ParaforGB[0] = args[0];
           RelationshipGraphBuilder.main(ParaforGB);

           String[] ParaforLP = {"",""};
           for(int i = 0; i < times; i++){
               ParaforLP[0] = args[1] + "/Data" + Integer.toString(i);
               ParaforLP[1] = args[1] + "/Data" + Integer.toString(i + 1);
               LabelPropagationer.main(ParaforLP);
           }
        } catch (Exception e) { e.printStackTrace(); }
    }
}
