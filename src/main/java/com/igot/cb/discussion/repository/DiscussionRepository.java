package com.igot.cb.discussion.repository;

import com.igot.cb.discussion.entity.DiscussionEntity;
import jakarta.transaction.Transactional;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface DiscussionRepository extends JpaRepository<DiscussionEntity, String>{

    @Modifying
    @Transactional
    @Query(value = "UPDATE discussion SET profanityresponse = cast(?2 as jsonb), isprofane = ?3 WHERE discussion_id = ?1", nativeQuery = true)
    void updateProfanityFieldsByDiscussionId(String discussionId, String profanityResponseJson, Boolean isProfane);

    @Query(
            value = "SELECT discussion_id, data, is_active, created_on, updated_on, up_vote_count, answer_post_count, down_vote_count   " +
                    "FROM discussion WHERE discussion_id = ?1",
            nativeQuery = true
    )
    Optional<DiscussionEntity> findDiscussionBasedOnDiscussionId(String discussionId);
}
