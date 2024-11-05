package com.mitocode.microservices.audit_service.repository;

import com.mitocode.microservices.common_models.model.entty.ProductEntity;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface ProductRepository extends CrudRepository<ProductEntity, Integer> {
}
