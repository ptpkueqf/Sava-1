package sava;

import java.io.Serializable;

public class Message implements Serializable {

    //source vertex who send the message
    private int sourceVertexID;

    //destination vertex of this message
    private int destVertexID;

    //message value
    private double value;

    /**
     * @return return the source vertex of this message
     */
    public int getSourceVertexID() {
        return sourceVertexID;
    }

    /**
     * @return return the destination vertex of this message
     */
    public int getDestVertexID() {
        return destVertexID;
    }

    /**
     * @return return the value of this message
     */
    public double getValue() {
        return value;
    }

    /**
     *
     * @param sourceVertexID
     * @param destVertexID
     * @param value
     */
    public Message(int sourceVertexID, int destVertexID, double value) {
        this.sourceVertexID = sourceVertexID;
        this.destVertexID = destVertexID;
        this.value = value;
    }


}
