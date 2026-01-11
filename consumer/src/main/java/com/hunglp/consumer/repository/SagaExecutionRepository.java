package com.hunglp.consumer.repository;

import com.hunglp.consumer.entity.SagaExecution;
import com.hunglp.consumer.saga.SagaState;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface SagaExecutionRepository extends JpaRepository<SagaExecution, String> {
    Optional<SagaExecution> findBySagaId(String sagaId);
    Optional<SagaExecution> findByOrderId(String orderId);
    List<SagaExecution> findByCurrentState(SagaState state);
}
