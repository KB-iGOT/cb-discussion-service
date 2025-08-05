package com.igot.cb.discussion.repository;

import com.igot.cb.discussion.entity.DiscussionEntity;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

@Repository
public interface DiscussionRepository extends JpaRepository<DiscussionEntity, String>{

    @Modifying
    @Transactional
    @Query(value = "UPDATE discussion SET profanityresponse = cast(?2 as jsonb), isprofane = ?3 WHERE discussion_id = ?1", nativeQuery = true)
    void updateProfanityFieldsByDiscussionId(String discussionId, String profanityResponseJson, Boolean isProfane);

}
