package com.hmdp.utils;

import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class LoginInterceptor implements HandlerInterceptor {
    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        // DEL session
//        // 1.获取session
//        HttpSession session = request.getSession();
//        // 2.获取用户
//        Object user = session.getAttribute("user");
//        // 3.用户是否存在？
//        if (user == null) {
//            // 拦截
//            response.setStatus(401);
//            return false;
//        }
//        // 4.存在，存入ThreadLocal, 使用UserHolder
//        UserHolder.saveUser((UserDTO) user);
//        // 5.放行
//        return true;

//        // 1.请求头token
//        String token = request.getHeader("authorization");
//        // 2.基于redis取user
//        if (StrUtil.isBlank(token)) {
//            response.setStatus(401);
//            return false;
//        }
//        String key = RedisConstants.LOGIN_USER_KEY + token;
//        Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);
//        if (userMap.isEmpty()) {
//            response.setStatus(401);
//            return false;
//        }
//        // 3.数据转换
//        UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
//        // 4.存入ThreadLocal
//        UserHolder.saveUser(userDTO);
//        // 5.刷新token有效期
//        stringRedisTemplate.expire(key, RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);


        // 1.判断是否需要拦截
        if (UserHolder.getUser() == null) {
            response.setStatus(401);
            return false;
        }
        // 2.有用户放行
        return true;
    }
}
