package com.jarvis.core.criteria;


import java.util.LinkedList;
import java.util.List;

public class TermCriteria {

    private String term;

    private List<String> mustFields = new LinkedList<String>();

    private TermCriteria nextTermCriteria;

    public TermCriteria(String term, String... mustFields) {
        this.term = term;
        addMustFields(mustFields);
    }

    // Copy Constructor
    private TermCriteria(TermCriteria termCriteria){
        this.term = termCriteria.getTerm();
        this.mustFields.addAll(termCriteria.getMustFields());
        if(termCriteria.getNextTermCriteria() != null){
            this.nextTermCriteria = new TermCriteria(termCriteria.getNextTermCriteria());
        }
    }

    public static TermCriteria clone(TermCriteria cloneFrom){
        return new TermCriteria(cloneFrom);
    }

    public String getTerm() {
        return term;
    }

    public List<String> getMustFields() {
        return mustFields;
    }

    public TermCriteria getNextTermCriteria() {
        return nextTermCriteria;
    }

    private void addMustFields(String... mustFields){
        for(String f : mustFields){
            this.mustFields.add(f);
        }
    }

    public void merge(TermCriteria that){
        getTail(this).nextTermCriteria = that;
    }

    private TermCriteria getTail(TermCriteria termCtriteria){
        if(termCtriteria.getNextTermCriteria() == null)
            return termCtriteria;
        else
            return getTail(termCtriteria.getNextTermCriteria());
    }


    @Override
    public String toString() {
        if(nextTermCriteria != null)
            return String.format("TC[{%15s} %10s --> tc=%s]", term, mustFields, nextTermCriteria);
        else
            return String.format("TC[{%15s} %10s]", term, mustFields);
    }
}
