package edu.cornell.cs.weijia.bs.test;

import java.util.*;

public class TestLab {

  public static void main(String args[]){
    LinkedList<String> ll = new LinkedList<String>();
    ll.add("A");
    ll.add("B");
    ll.add("C");
    ListIterator<String> li = ll.listIterator(ll.size());
    System.out.println(li.previous());
  }
}
