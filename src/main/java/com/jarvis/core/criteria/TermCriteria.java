package com.jarvis.core.criteria;


import java.util.LinkedList;
import java.util.List;

public class TermCriteria {

    private String term;

    private List<String> mustFields = new LinkedList<String>();

    private TermCriteria childTermCriteria;

    public TermCriteria(String term, TermCriteria childTermCriteria, String... mustFields) {
        this(term, mustFields);
        this.childTermCriteria = childTermCriteria;
    }

    public TermCriteria(String term, String... mustFields) {
        this.term = term;
        addMustFields(mustFields);
    }

    private void addMustFields(String... mustFields){
        for(String f : mustFields){
            this.mustFields.add(f);
        }
    }
}
