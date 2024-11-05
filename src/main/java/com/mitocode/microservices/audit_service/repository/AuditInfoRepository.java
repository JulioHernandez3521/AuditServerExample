package com.mitocode.microservices.audit_service.repository;

import com.mitocode.microservices.common_models.model.entty.AuditInfo;
import org.springframework.data.repository.CrudRepository;

public interface AuditInfoRepository extends CrudRepository<AuditInfo, Long> {
}
