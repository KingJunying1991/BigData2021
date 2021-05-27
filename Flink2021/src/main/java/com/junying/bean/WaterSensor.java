package com.junying.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * WaterSensor
 * 水位传感器：用于接收水位数据
 * <p>
 *    id:传感器编号
 *    ts:时间戳
 *    vc:水位
 * @author King
 * @date 2021/5/21 11:45
 * @since 1.0.0
 */


@Data
@NoArgsConstructor
@AllArgsConstructor
public class WaterSensor {
    private String id;
    private Long ts;
    private Integer vc;
}
