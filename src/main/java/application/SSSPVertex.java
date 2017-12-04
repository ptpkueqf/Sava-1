package application;

import sava.Message;
import sava.Vertex;

import java.util.List;

public class SSSPVertex extends Vertex  {

    private int sourceVertexID = 1;

    private int previousVertexID;

    private boolean sourceFlag;

    private int vertexID;

    public SSSPVertex() {

        this.setValue(Double.MAX_VALUE);
        previousVertexID = this.getVertexID();
    }

    public void setVertexID(int vertexID) {
        super.setVertexID(vertexID);
        if (this.getVertexID() == sourceVertexID) {
            this.setValue(0);
        }
    }

    private boolean isSourceVertex() {
        return this.getVertexID() == sourceVertexID;
    }

    public void compute(int superstep) {
        //clear the outputmessages first

        //each time, clear inputmessages finally
        double min = Double.MAX_VALUE;
        int newPreviousVertex = -1;
        for (Message msg : this.getInputMessages()) {
            if (msg.getValue() < min) {
                min = msg.getValue();
                newPreviousVertex = msg.getSourceVertexID();
            }
        }

        List<Message> outmessages = this.getOutputMessages();
        outmessages.clear();
        if (min < this.getValue()) {
            this.previousVertexID = newPreviousVertex;
            this.setValue(min + 1);

            for (Integer i : this.getOutVertex()) {
                outmessages.add(new Message(this.getVertexID(), i, this.getValue()));
            }
        }

        this.getInputMessages().clear();
    }

    public String toString() {
//        return this.getVertexID() + " shortest pathcost: " + this.getValue() + ", previous vertex" + this.previousVertexID + "\n";
        return this.getVertexID() + "    " + this.getValue() + "\n";
    }
}
