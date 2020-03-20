        insert into ynga.emergency_msg_send(msg,msg_time,state,send_time)
        select 
        emergency_reason,emergency_date,0,SYSDATE()
        from controls.emergency_info_tmp A
        inner join controls.object_info B 
        on A.object_business_code=B.object_business_code
        inner join controls.object_type_relation C 
        on B.id=C.object_id
        where C.object_type_id=20000 and (emergency_reason like '%进入%北京%' or emergency_reason like '%长水机场%' or emergency_reason like '%昆明站%' or emergency_reason like '%昆明南站%')
