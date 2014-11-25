node-rtmp
=========
An RTMP Server implemented by pure NodeJS.  
  feature:  
  support AMF0  
  support multi-stream  
  support edge-origin:  forward RTMP stream to origin-server  

extended-timestamp issue: https://code.google.com/p/red5/issues/detail?id=107

When test with 1 publisher, 1000 subscribers, bandwidth : 400 Mbps, 99.29%'s time is spend on write system call.  
From https://github.com/winlinvip/simple-rtmp-server/issues/194, we can see that writev can improve the performance.  

~/st-load$ sudo strace -p 19869 -c  
Process 19869 attached - interrupt to quit  
^CProcess 19869 detached  
% time     seconds  usecs/call     calls    errors syscall  
------ ----------- ----------- --------- --------- ----------------  
 99.29    0.018917           0   1209145           write  
  0.53    0.000101           0       941           read  
  0.13    0.000025           0     60000           epoll_ctl  
  0.05    0.000010           0      7491           futex  
  0.00    0.000000           0       128           mmap  
  0.00    0.000000           0       180           munmap  
  0.00    0.000000           0         4           brk  
  0.00    0.000000           0       250           epoll_wait  
------ ----------- ----------- --------- --------- ----------------  
100.00    0.019053               1278139           total  

RTMP server implemented by nodejs 
