use std::{cell::RefCell, rc::Rc};


type Link<T> = Option<Rc<RefCell<Box<ListNode<T>>>>>;

pub struct ListNode<T> {
    val: T,
    next: Link<T>,
    prev: Link<T>,
}

impl<T> ListNode<T> {
    fn new(val: T) -> Self {
        ListNode {
            val,
            next: None,
            prev: None,
        }
    }

    fn set_prev(&mut self, prev: &Link<T>) {
        self.prev = prev.clone();
    }
    fn set_next(&mut self, next: &Link<T>) {
        self.next = next.clone();
    }
    fn get_prev(&self) -> Link<T> {
        self.prev.clone()
    }
    fn get_next(&self) -> Link<T> {
        self.next.clone()
    }

    fn new_link(data: T) -> Link<T> {
        Some(Rc::new(RefCell::new(Box::new(ListNode::new(data)))))
    }
}



pub struct List<T> {
    head: Link<T>,
    tail: Link<T>,
    len: usize,
}

impl<T> List<T> {
    fn new() -> Self {
        List {
            head: None,
            tail: None,
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn push_tail(&mut self, data: T) {
        let mut new_node = ListNode::new_link(data);
        self.len += 1;
        if self.head.is_none() && self.tail.is_none() {
            self.head = new_node.clone();
            self.tail = new_node.clone();
            return;
        }
        
        let tail = self.tail.clone().unwrap();
        let mut tail = tail.borrow_mut();
        tail.set_next(&new_node);
        new_node.as_ref().unwrap().borrow_mut().set_prev(&self.tail);
        self.tail = new_node;
    }

    fn push_head(&mut self, data: T) {
        let mut new_node = ListNode::new_link(data);
        self.len += 1;
        if self.head.is_none() && self.tail.is_none() {
            self.head = new_node.clone();
            self.tail = new_node.clone();
            return;
        }
        
        let head = self.head.clone().unwrap();
        let mut head = head.borrow_mut();
        head.set_prev(&new_node);
        new_node.as_ref().unwrap().borrow_mut().set_next(&self.head);
        self.head = new_node;
    }
}
