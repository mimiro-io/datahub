JEMALLOC_URL="https://github.com/jemalloc/jemalloc/releases/download/5.2.1/jemalloc-5.2.1.tar.bz2"
mkdir -p /tmp/jemalloc-temp && cd /tmp/jemalloc-temp ; 
echo "Downloading jemalloc" ; 
curl -s -L ${JEMALLOC_URL} -o jemalloc.tar.bz2 ;
tar xjf ./jemalloc.tar.bz2 ; 
cd jemalloc-5.2.1 ; 
./configure --with-jemalloc-prefix='je_' --with-malloc-conf='background_thread:true,metadata_thp:auto'; 
make ; 
make install ; 
