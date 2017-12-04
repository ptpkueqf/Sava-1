package application;

import sava.Message;
import sava.Vertex;

import java.util.ArrayList;
import java.util.List;

public class PageRankVertex extends Vertex implements Comparable{

    public PageRankVertex() {
        this.setValue(1.0);
    }

    public void compute(int superstep) {
        //clear the outputmessages first
        //each time, clear inputmessages finally
        this.getOutputMessages().clear();

        if (superstep >= 1) {
            double sum = 0.0;
            for (Message message : getInputMessages()) {
                sum += message.getValue();
            }
            double pagerank = 0.85 * sum + 0.15 / getOutVertex().size();
            this.setValue(pagerank);
        }

        if (superstep <= 20) {
            List<Message> outputMessages = this.getOutputMessages();
            for (Integer i : this.getOutVertex()) {
                outputMessages.add(new Message(this.getVertexID(), i, this.getValue()));
            }
        }
        this.getInputMessages().clear();
    }

    public String toString() {
        return this.getVertexID() + "    " + this.getValue() + "\n";
    }

    public int compareTo(Object obj) {
        PageRankVertex pageRankVertex = (PageRankVertex) obj;
        return (int)((pageRankVertex.getValue() - this.getValue()) * 10);
    }
}
