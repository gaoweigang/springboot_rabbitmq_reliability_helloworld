package com.gwg.demo.mq.message;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by 
 */
//for example
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserMessage {
    int id;
    
    //用户姓名
    String name;
}
