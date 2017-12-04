package sava;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public abstract class Vertex implements Serializable, Comparable{

    private int vertexID;

    private List<Message> inputMessages;

    private List<Message> outputMessages;

    private double value;

    private List<Integer> outVertex = new ArrayList<Integer>();

    public Vertex(){
        inputMessages = new ArrayList<Message>();
        outputMessages = new ArrayList<Message>();
    }

    /**
     * return the unique id of this vertex
     * @return
     */
    public int getVertexID() {
        return vertexID;
    }

    /**
     * set id for the vertex
     * @param vertexID
     */
    public void setVertexID(int vertexID) {
        this.vertexID = vertexID;
    }

    /**
     * get the vertex value
     * @return
     */
    public double getValue() {
        return value;
    }

    /**
     * set the vertex value for this vertex
     * @param value
     */
    public void setValue(double value) {
        this.value = value;
    }

    /**
     *
     * @return
     */
    public List<Integer> getOutVertex() {
        return outVertex;
    }

    public void addMessage(Message msg) {
        inputMessages.add(msg);
    }

    public List<Message> getInputMessages() {
        return inputMessages;
    }

    public List<Message> getOutputMessages() { return outputMessages;}

    /**
     *
     * @param outVertex
     */
    public void setOutVertex(List<Integer> outVertex) {
        this.outVertex = outVertex;
    }

    /**
     * initalize the vertex with id, value and a list of adjacent vertexes
     * @param vertexID
     * @param value
     * @param outVertex
     */
    public void initialize(int vertexID, double value, List<Integer> outVertex) {
        this.vertexID = vertexID;
        this.value = value;
        this.outVertex = outVertex;
        if (outVertex == null) {
            this.outVertex = new ArrayList<Integer>();
        }
    }


//    public String toString() {
//        String res = this.getVertexID() + " => ";
//        boolean flag = true;
//        for (Integer i : outVertex) {
//            if (flag) {
//                res += i.toString();
//                flag = false;
//                continue;
//            }
//            res = res + "," + i.toString();
//        }
//        return res;
//    }

    /**
     * this method should be overriden by application developer according to the application demand
     */
    public abstract void compute(int superstep);

    public int compareTo(Object obj) {
        Vertex vertex = (Vertex)obj;
        return (int)((vertex.getValue() - this.getValue()) * 10);
    }

}
