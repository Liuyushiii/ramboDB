
#include "Rambo_construction.h"
#include "util/hash.h"
using namespace std;

vector<uint> RAMBO::hashfunc(std::string key, int len) {
    // int hashvals[k];
    vector<uint> hashvals;
    uint op;
    // op=std::atoi(key.c_str());
    // for(int i=0;i<R;i++)
    // {
    //     hashvals.push_back(op/2 %B);
    // }
    // return hashvals;
    for (int i = 0; i < R; i++) {
        MurmurHash3_x86_32(key.c_str(), len, i, &op); //seed i
        //op=leveldb::Hash(key.c_str(),key.size(),i);
        hashvals.push_back(op % B);
    }
    return hashvals;
}

std::vector<std::string> RAMBO::getdata(string filenameSet) {
    //get the size of Bloom filter by count
    ifstream cntfile(filenameSet);
    std::vector<std::string> allKeys;
    int totKmerscnt = 0;
    while (cntfile.good()) {
        string line1, vals;
        while (getline(cntfile, line1)) {
            stringstream is;
            is << line1;
            if (line1[0] != '>' && line1.size() > 30) {
                for (uint idx = 0; idx < line1.size() - 31 + 1; idx++) {
                    allKeys.push_back(line1.substr(idx, 31));
                    totKmerscnt++;
                }
            }
        }
    }
    std::cout << "total inserted from one file: " << totKmerscnt << std::endl;
    return allKeys;
}


RAMBO::RAMBO(int n, int r1, int b1, int K,int bias_) {
    R = r1;
    B = b1;
    K1 = K;
    bias = bias_;

    p = 0.01; //false positive rate
    range = ceil(-(n * log(p)) / (log(2) * log(2))); //range
    //std::cout<<"BF_size: "<<range<<std::endl;
    // range = n;
    // k = 3;


    k = ceil(range/n * log(2)); //number of hash, k is 7 for 0.01
    //std::cout<<"range: "<<range<<" k: "<<k<<std::endl;
    Rambo_array = new BloomFiler *[B * R]; //array of pointers
    //metaRambo = new vector<int>[B * R]; //constains set info in it.
    //metaRambo = new unordered_set<int>[B * R];
    metaRambo = new int64_t[B * R];
    for (int b = 0; b < B; b++) {
        for (int r = 0; r < R; r++) {
            Rambo_array[b + B * r] = new BloomFiler(range, p, k);
            metaRambo[b+B*r]=0;
        }
    }
}

void RAMBO::serializeRAMBO(string dir) {
    for (int b = 0; b < B; b++) {
        for (int r = 0; r < R; r++) {
            string br = dir + to_string(b) + "_" + to_string(r) + ".txt";
            Rambo_array[b + B * r]->serializeBF(br);
        }
    }
}

std::string RAMBO::toString(){
    std::string buffer;
    buffer.reserve((range/8+1)*B*R);
    for(int b=0;b<B;++b){
        for(int r=0;r<R;++r){
            buffer.append(std::string(Rambo_array[b+B*r]->m_bits->A,range/8+1));
        }
    }
    return std::move(buffer);
}

bool RAMBO::decodeFrom(const char* str,size_t str_size){
    int size_per_bloom=(range/8+1);
    int size=size_per_bloom*B*R;
    if(str_size==size){
        int count=0;
        for(int b=0;b<B;++b){
            for(int r=0;r<R;++r){
                memcpy(Rambo_array[b+B*r]->m_bits->A,str+count*size_per_bloom,size_per_bloom);
                count++;
            }
        }

        return true;
    }else{
        return false;
    }
}

void RAMBO::deserializeRAMBO(vector<string> dir) {
    for (int b = 0; b < B; b++) {
        for (int r = 0; r < R; r++) {
            vector<string> br;
            for (uint j = 0; j < dir.size(); j++) {
                br.push_back(dir[j] + to_string(b) + "_" + to_string(r) + ".txt");
            }
            Rambo_array[b + B * r]->deserializeBF(br);
        }
    }
}

void RAMBO::createMetaRambo_single(int value, int64_t bit){
        vector<uint> hashvals = RAMBO::hashfunc(std::to_string(value),
                                                std::to_string(value).size()); // R hashvals, each with max value B
        for (int r = 0; r < R; r++) {
            //metaRambo[hashvals[r] + B * r].push_back(value);
            //metaRambo[hashvals[r] + B * r].insert(value);
            metaRambo[hashvals[r] + B * r]|=((int64_t)1 << bit);
        }
}


void RAMBO::insertion_pairs(std::vector<std::pair<std::string, std::string>> &data_key_number) {
    for (auto pair: data_key_number) {
        string temp = to_string(atoi(pair.second.c_str()));
        vector<uint> hashvals = RAMBO::hashfunc(temp, temp.size());
        for (int r = 0; r < R; r++) {
            vector<uint> temp = myhash(pair.first, pair.first.size(), k, r, range);// i is the key
            Rambo_array[hashvals[r] + B * r]->insert(temp);
        }
    }
}

void RAMBO::insertion_pair(std::pair<std::string, std::string> pair1){
        string temp = to_string(atoi(pair1.second.c_str()));
        vector<uint> hashvals = RAMBO::hashfunc(temp, temp.size());
        for (int r = 0; r < R; r++) {
            vector<uint> temp = myhash(pair1.first, pair1.first.size(), k, r, range);// i is the key
            Rambo_array[hashvals[r] + B * r]->insert(temp);
            // uint op;
            // MurmurHash3_x86_32(pair1.first.c_str(),pair1.first.size(),1,&op);
            // metaRambo[(op % 5)+B*r*5]
        }
}

boost::dynamic_bitset<> RAMBO::query_bias(std::string query_key, int len, int bias) {
    //chrono::time_point<chrono::high_resolution_clock> tstart = chrono::high_resolution_clock::now();

    // set<int> resUnion[R]; //constains union results in it.
    //bitArray bitarray_K(K1);
    boost::dynamic_bitset<> bitarray_K(K1+1);
    // bitset<Ki> bitarray_K;
    // set<int> res;
    //float count=0.0;
    vector<uint> check;
    for (int r = 0; r < R; r++) {
        check = myhash(query_key, len, k, r, range); //hash values correspondign to the keys
        // bitArray bitarray_K1(Ki);
        //bitArray bitarray_K1(K1);
        boost::dynamic_bitset<> bitarray_K1(K1+1);
        // bitset<Ki> bitarray_K1;
        //chrono::time_point<chrono::high_resolution_clock> t1 = chrono::high_resolution_clock::now();
        for (int b = 0; b < B; b++) {
            if (Rambo_array[b + B * r]->test(check)) {
                //chrono::time_point<chrono::high_resolution_clock> t5 = chrono::high_resolution_clock::now();
                
                // for (uint j = 0; j < metaRambo[b + B * r].size(); j++) {
                //       //bitarray_K1.SetBit(metaRambo[b + B * r][j] - bias);
                //       bitarray_K1.set(metaRambo[b + B * r][j] - bias);
                // }
                
                // for(auto x:metaRambo[b + B * r]){
                //     bitarray_K1.set(x - bias);
                // }

                //chrono::time_point<chrono::high_resolution_clock> t6 = chrono::high_resolution_clock::now();
                //count+=((t6-t5).count()/1000000000.0);
            }
        }

        //chrono::time_point<chrono::high_resolution_clock> t2 = chrono::high_resolution_clock::now();
        
        
        if (r == 0) {
            bitarray_K = bitarray_K1;
        } else {
            //bitarray_K.ANDop(bitarray_K1.A);

            bitarray_K = bitarray_K&bitarray_K1;
            //bitarray_K1.bitArray_delete();

        }
        //chrono::time_point<chrono::high_resolution_clock> t3 = chrono::high_resolution_clock::now();
        
        //cout<<"sum set "<<count<<endl;
        //cout<<"do all set:"<<((t2-t1).count()/1000000000.0)<<endl;
        //cout<<"do and:"<<((t3-t2).count()/1000000000.0)<<endl;
        
    }
    //vector<uint>().swap(check);
    
    //chrono::time_point<chrono::high_resolution_clock> tend = chrono::high_resolution_clock::now();
    //cout<<"whole delta:"<<((tend-tstart).count()/1000000000.0)<<endl;
    
    return bitarray_K;
}

set<int> RAMBO::query_bias_set(std::string query_key, int len) {

    
    // arena_.execute([&]{
    //     tbb::parallel_for(tbb::blocked_range<int>(0,R),[&](const tbb::blocked_range<int> &_r){
    //         for(int r=_r.begin();r!=_r.end();r++)
    //         {
    //             vector<uint> check=myhash(query_key,len,k,rc,range);
    //             bitArray_K1[r].resize(K1+1,false);
    //             for(int b=0;b<B;b++)
    //             {
    //                 if(Rambo_array[b+B*r]->test(check)){
    //                     for(auto x:metaRambo[b+B*r]){
    //                         bitArray_K1[r].set(x-bias);
    //                     }
    //                 }
    //             }
    //         }

    //     });
    // });
    //boost::dynamic_bitset<> bitArray_K1[R];
    int64_t bit_ary[R];
    for(int r=0;r<R;r++)
    {
        
        vector<uint> check=myhash(query_key,len,k,r,range);
        //bitArray_K1[r].resize(K1+1,false);
        bit_ary[r]=0;
        //std::cout<<"r: "<<r<<std::endl;
        for(int b=0;b<B;b++)
        {
            //std::cout<<b<<" : ";
            if(Rambo_array[b+B*r]->test(check)){
                //std::cout<<b<<" : ";
                
                // for(auto x:metaRambo[b+B*r]){
                //     //std::cout<<x<<" ";
                //     bitArray_K1[r].set(x-bias);
                // }
                
                bit_ary[r]=bit_ary[r]|metaRambo[b+B*r];
                //std::cout<<std::endl;
            }
           // std::cout<<std::endl;
        }
    }
    for(int i=1;i<R;i++)
    {
        //bitArray_K1[0]=bitArray_K1[0]&bitArray_K1[i];
        bit_ary[0]=bit_ary[0]&bit_ary[i];
    }
    //std::cout<<"YES2"<<std::endl;
    set<int> temp;
    int d=0;
    int64_t s=bit_ary[0];
    while(s)
    {
        if(s&1)
        {
            temp.emplace(d);
        }
        s/=2;
        d++;
    }
    // for(size_t fid=bitArray_K1[0].find_first();fid!=boost::dynamic_bitset<>::npos;){
    //     //std::cout<<fid+bias<<" ";
    //     temp.emplace(bias+fid);
    //     fid=bitArray_K1[0].find_next(fid);
    // }
    //std::cout<<"YES3"<<std::endl;
    return temp;
//     set<int> resUnion[R]; //constains union results in it.
    

//     // vector<int> ve;
//     // for(int i=0;i<R;i++)
//     //     ve.emplace_back(i);

//     // tbb::parallel_for_each(ve.begin(), ve.end(), [&](int r){
//     //             //std::cout << r.i << std::endl; 
//     //     vector<uint> check; 
//     //     check=myhash(query_key, len, k, r, range);
//     //     for (int b = 0; b < B; b++) {
//     //         if (Rambo_array[b + B * r]->test(check)) {
//     //             for(auto x:metaRambo[b + B * r]){
//     //                 //bitarray_K1.set(x - bias);
//     //                 resUnion[r].emplace(x);
//     //             }
//     //         }
//     //     }
//     // });

//     vector<uint> check;
//     for (int r = 0; r < R; r++) {
//         // chrono::time_point<chrono::high_resolution_clock> t11 = chrono::high_resolution_clock::now();
//         check = myhash(query_key, len, k, r, range); //hash values correspondign to the keys
//         // chrono::time_point<chrono::high_resolution_clock> t12 = chrono::high_resolution_clock::now();
//         for (int b = 0; b < B; b++) {
//             if (Rambo_array[b + B * r]->test(check)) {
//                 for(auto x:metaRambo[b + B * r]){
//                     //bitarray_K1.set(x - bias);
//                     resUnion[r].emplace(x);
//                 }
//             }
//         }
//     }
//   //  std::cout<<"end!!"<<std::endl;
//     for(int i=1;i<R;i++){
//         set<int> temp;
//         std::set_intersection(resUnion[i-1].begin(), resUnion[i-1].end(),
//                           resUnion[i].begin(), resUnion[i].end(),
//                           std::inserter(temp, temp.begin()));
//         resUnion[i] = temp;
//     }
    
//     vector<uint>().swap(check);
//     return resUnion[R-1];
}


void RAMBO::merge_another_rambo(RAMBO &b){
    K1 += b.K1;
    for(int i=0;i<B * R;i++){
        Rambo_array[i]->merge_another_bf(b.Rambo_array[i]);
        //metaRambo[i].insert(b.metaRambo[i].begin(),b.metaRambo[i].end());
        metaRambo[i]=metaRambo[i]|b.metaRambo[i];
    } 
};

void RAMBO::out_set(){
    for(int r=0;r<R;r++)
    {
        std::cout<<"R: "<<r<<std::endl;;
        for(int b=0;b<B;b++)
        {
            // for(auto x:metaRambo[b+B*r]){
            //     std::cout<<x<<" ";
            // }
            int s=metaRambo[b+B*r];
            int d=0;
            while(s)
            {
                if(s&1)
                {
                    std::cout<<bias+d<<" ";
                }
                s/=2;
                d++;
            }
            std::cout<<std::endl;
        }
    }
}