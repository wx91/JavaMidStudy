#/bin/ls
/bin/date +%F >> /test/ex2/info
echo "disk info:" >> /test/ex2.info
/bin/df -h >> /test/ex2.info
echo "online users:" >> /test/ex2.info
/usr/bin/who | /bin/grep -v root >> /test/ex2.info
echo "memory info:" >> /test/ex2.info
/usr/bin/free -m >> /test/ex2.info
echo >> /test/ex2.info
#write root
/usr/bin/write root < /test/ex2.info && /bin/rm /test/ex2.info
# crontab -e
# 0 9 * * 1-5 /bin /sh /test/tex.info