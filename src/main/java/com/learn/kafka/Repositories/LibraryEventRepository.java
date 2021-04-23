package com.learn.kafka.Repositories;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import com.learn.kafka.Models.LibraryEvent;

@Repository
public interface LibraryEventRepository extends JpaRepository<LibraryEvent, Integer> {

}
