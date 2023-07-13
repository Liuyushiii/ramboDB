/* Copyright (c) 2011 The LevelDB Authors. All rights reserved.
   Use of this source code is governed by a BSD-style license that can be
   found in the LICENSE file. See the AUTHORS file for names of contributors. */

#include "leveldb/db.h"
#include "leveldb/env.h"
#include "db/version_edit.h"
#include "leveldb/write_batch.h"
#include "leveldb/cache.h"
#include "db/memtable.h"
#include "codec/Common.h"
#include <cmath>
#include <openssl/sha.h>
#include <rambo/Rambo_construction.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <codec/RowWriter.h>
#include <ctime>
#include "util/hash.h"
#include <nlohmann/json.hpp>
#include <unordered_map>
#include <boost/asio.hpp>
#define ACCOUNT_RANGE 1000000
#define random(x) (rand()%x)
std::string sha256(const std::string &srcStr) {
  unsigned char mdStr[33] = {0};
  SHA256((const unsigned char *)srcStr.c_str(), srcStr.length(), mdStr);
  return std::string((char *)mdStr, 32);
}

using json = nlohmann::json;
using boost::asio::ip::tcp;
using leveldb::PropertyType ;
using leveldb::Value ;


struct Tx{
  std::string from_address;
  std::string to_address;
  int64_t value;
}tx[1005];
bool cmp(Tx a, Tx b)
{
  return a.from_address<b.from_address;
}
int cnt=0;

std::map<std::string,int64_t>address_mp;
std::map<std::string,std::string>mmp;
std::vector<std::pair<std::string,std::int64_t>>address_ve;
struct Testo{
  std::string add_ori;
  std::string add_new;
  int64_t l;
  int64_t range;
  int64_t step;
  Testo(){};
  Testo(std::string a, std::string b, int64_t c, int64_t d, int64_t e)
  {
    add_ori=a;
    add_new=b;
    l=c;
    range=d;
    step=e;
  };
}testo[1005]/*,cold[200005],mid[200005],hot[200005]*/,testq[20000005];
int tq;
int nc,nm,nh;
double init_time=0,add_time=0;
int64_t init_sum=0,add_sum=0;

void generateTx(std::string &key, std::string &value, std::vector<Tx>ve, int64_t version)
{
  key=ve[0].from_address;
  int64_t vv=leveldb::ramboreverse_int64(version);
  key.append(reinterpret_cast<char*>(&vv),sizeof(int64_t));

  leveldb::ResultSchemaProvider rsp;
  int64_t num=ve.size();
  clock_t begin, end;
  double ans;
  begin = clock();
  leveldb::RowWriter r(&rsp,num); 
  end = clock();
  ans=double(end - begin) / CLOCKS_PER_SEC *1000;    
  init_time+=ans;
  init_sum++;
  leveldb::WriteResult wRet;
  for(int pos=0;pos<num;pos++)
  {
    Value from(ve[pos].from_address);
    Value to(ve[pos].to_address);
    Value v(ve[pos].value);
    begin = clock();
    wRet=r.setDstId(pos+1,ve[pos].to_address);
    wRet=r.setVersion(pos+1,version);
    wRet=r.setMultiValue(pos+1,0,from);
    wRet=r.setMultiValue(pos+1,1,to);
    wRet=r.setMultiValue(pos+1,2,v);
    end = clock();
    ans=double(end - begin) / CLOCKS_PER_SEC *1000;
    add_time+=ans;
    add_sum++;
  }
  value=r.getEncodedStr();
}
int64_t generateblockWriteBatch(int64_t block, leveldb::WriteBatch &write_batch_) {

  std::vector<Tx>ve;
  ve.emplace_back(tx[1]);
  std::string pre=tx[1].from_address;
  int64_t sum=0;
  std::string key;
  std::string value;
  for(int i=2;i<=cnt;i++)
  {
    std::string a=tx[i].from_address;
    if(a==pre)
    {
      ve.emplace_back(tx[i]);
    }else{
      generateTx(key,value,ve,block);
      write_batch_.Put(key,value,block);
      ve.clear();
      ve.emplace_back(tx[i]);
      pre=a;
      sum++;
    }
  }
  generateTx(key,value,ve,block);
  write_batch_.Put(key,value,block);
  sum++;
  ve.clear();
  write_batch_.RecordRange();
  return sum;
}



// enum value_type{
  
// };
//
std::map<std::string,std::string> address_f;
struct EdgeResult{
    std::string from_address;
    std::string to_address;
    int64_t block;
    int64_t value;
    std::vector<int> value_type;
    std::vector<Value> values;
    EdgeResult(){};
    EdgeResult(std::string from, std::string to, int64_t b,int64_t v)
    {
      from_address=from;
      to_address=to;
      block=b;
      value=v;
    };
    void addValue(int type, Value value)
    {
      this->value_type.emplace_back(type);
      this->values.emplace_back(value);
    }
};
struct NodeResult{
  std::string address;
  NodeResult(){};
  NodeResult(std::string a)
  {
    address=a;
  }
};
std::vector<NodeResult> nodes;
std::vector<EdgeResult> edges;

// 模拟node数据
std::string get_nodes(){
      json arr = json::array();
      std::cout<<"size of nodes: "<<nodes.size()<<std::endl;
      for(int i=0;i<nodes.size();i++)
      {
        arr.push_back(json{
            {"id", nodes[i].address},
            {"degree", 0},
            {"cluster", 0}
        });
      }
      std::string json_data = arr.dump(); // 转换为字符串
      return "\"nodes\":"+json_data;
}
// 模拟edge数据
std::string get_edges(){
      json arr = json::array();
      std::cout<<"size of edges: "<<edges.size()<<std::endl;
      for(int i=0;i<edges.size();i++)
      {
        arr.push_back(json{
          {"source", edges[i].from_address}, 
          {"target", edges[i].to_address}, 
          {"value", edges[i].value},
          {"tx_hash", edges[i].to_address}
        });
      }
      std::string json_data = arr.dump(); // 转换为字符串
      return "\"edges\":"+json_data;
}

std::string get_edges_and_nodes(){
  std::stringstream ss;
  ss << "{";
  ss << get_edges();
  // ss << ",";
  // ss << get_nodes();
  ss << "}";
  return ss.str();
}
// 解析请求参数，将键值对存储到unordered_map中
std::unordered_map<std::string, std::string> parse_params(const std::string& request) {

  // std::cout << "request: " << request << std::endl;

  std::unordered_map<std::string, std::string> params_map;

  // 找到请求参数的起始位置
  size_t pos_start = request.find("?");
  size_t pos_end = request.find(" HTTP");

  // 如果没有参数，则直接返回
  if (pos_start == std::string::npos) {
    return params_map;
  }

  // 提取请求参数字符串
  std::string params = request.substr(pos_start + 1, pos_end - pos_start - 1);

  // 解析请求参数，将键值对存储到unordered_map中
  size_t sep_pos;
  std::string key, value;
  while ((sep_pos = params.find("&")) != std::string::npos) {
    key = params.substr(0, sep_pos);
    value = "";
    params.erase(0, sep_pos + 1);

    sep_pos = key.find("=");
    if (sep_pos != std::string::npos) {
      value = key.substr(sep_pos + 1);
      key = key.substr(0, sep_pos);
    }

    params_map[key] = value;
  }

  // 处理最后一个键值对
  sep_pos = params.find("=");
  if (sep_pos != std::string::npos) {
    key = params.substr(0, sep_pos);
    value = params.substr(sep_pos + 1);
    params_map[key] = value;
  }

  return params_map;
}



double k_top(leveldb::DB* db, std::string key, int64_t startv,int64_t endv, int64_t step)
{
  std::cout << "executing " << step << " hop on " << address_f[key] << " from " << startv << " to " << endv << std::endl;
  std::map<std::string,int>mp;
  leveldb::ReadOptions read_option;
  read_option.min_height=startv;
  read_option.max_height=endv;
  mp[key]=1;
  std::vector<std::string>q;
  std::vector<std::string>qq;
  q.emplace_back(key);
  int64_t sum=0;
  int64_t sss=0;
  double sum_ans=0,read_sumtime=0;
  double ans;
  //std::cout<<"block:"<<startv<<"-"<<endv<<std::endl;
  nodes.clear();
  edges.clear();
  for(int i=1;i<=step;i++)
  {
    if(q.size()==0)
      break;
    int64_t ss=0;
    //std::cout<<"----step: "<<i<<std::endl;
    for(int k=0;k<q.size();k++)
    {
      std::string value;
      value.append(8,'0');
      //std::cout<<q[k]<<" l:"<<startv<<" r:"<<endv<<std::endl;

      //记录点
      nodes.emplace_back(NodeResult(address_f[q[k]]));

      //std::cout<<"start:"<<address_f[q[k]]<<std::endl;
      clock_t begin, end;
      double ans;
      begin = clock();
      db->Get(read_option,q[k],&value);
      end = clock();
      ans=double(end - begin) / CLOCKS_PER_SEC *1000;
      //std::cout<<"each_time: "<<ans<<std::endl;
      sum_ans+=ans;
      sum++;
      //std::cout<<value.size()<<std::endl;
      if(value.size()==8)
        continue;
      begin = clock();
      leveldb::ResultSchemaProvider rsp;
      leveldb::RowWriter r(&rsp,std::move(value));
      int64_t edgenumber=r.getEdgenumber();
      ss+=edgenumber;
      //std::cout<<"start:"<<mmp[q[k]]<<std::endl;
      //go from "0xa696c12a3952e8977ed9a54c09abecfbf7fe641b" over tx yield properties(edge) version: 8155776 to 8308667;
      //std::cout<<"go from \"0x"<<mmp[q[k]]<<"\" over tx yield properties(edge) version: "<<edgenumber<<std::endl;
      //std::cout<<mmp[key]<<" "<<startv<<"-"<<endv<<" "<<edgenumber<<std::endl;
      if(i==step)
      {
        for(int j=1;j<=edgenumber;j++)
        {
          std::string dst=r.getPosDstId(j).getStr();
          std::int64_t ve=r.getPosVersion(j).getInt();
          std::int64_t va=r.getPosValueByIndex(j,2).getInt();
          // std::string s=r.getPosValueByIndex(j,0).getStr();
          // std::string e=r.getPosValueByIndex(j,1).getStr();
          
          //记录边
          edges.emplace_back(EdgeResult(address_f[q[k]],address_f[dst],ve,va));
          if(mp[dst]==0)
          {
            mp[dst]=1;
            nodes.emplace_back(NodeResult(address_f[q[k]]));
          }
        }
        
      }else{
        for(int j=1;j<=edgenumber;j++)
        {
          std::string dst=r.getPosDstId(j).getStr();
          std::int64_t ve=r.getPosVersion(j).getInt();
          std::int64_t va=r.getPosValueByIndex(j,2).getInt();
          // std::string s=r.getPosValueByIndex(j,0).getStr();
          // std::string e=r.getPosValueByIndex(j,1).getStr();
          
          //记录边
          //std::cout<<address_f[q[k]]<<"-"<<address_f[dst]<<":"<<ve<<std::endl;
          edges.emplace_back(EdgeResult(address_f[q[k]],address_f[dst],ve,va));
          //std::cout<<"from_account: \"0x"<<mmp[q[k]]<<"\", to_account: \"0x"<<mmp[dst]<<"\""<<std::endl;
          if(mp[dst]==0)
          {
            mp[dst]=1;
            qq.emplace_back(dst);
          }
        }
      }
      end = clock();
      ans=double(end - begin) / CLOCKS_PER_SEC *1000;
      read_sumtime+=ans;
    }
    //std::cout<<"step: "<<i<<" num: "<<ss<<std::endl;
    sss+=ss;
    q=qq;
    qq.clear();
  }
  // std::cout<<"sss="<<sss<<std::endl;
  // std::cout << "node: " << nodes.size() << std::endl;
  // std::cout << "edge: " << edges.size() << std::endl;
  // std::cout<<sum_ans+read_sumtime<<std::endl;
  //return sum;
  return sum_ans+read_sumtime;
}

int cas[10];
int q=0;
double rating_test(leveldb::DB* db,int64_t s, int64_t rate)
{
  double ans=0;
  int T=1;
  std::cout<<"----1hop----"<<std::endl;
  while(T--)
  {
    double sumans=0;
    for(int i=1;i<=10;i++)
    {
      sumans+=k_top(db,testo[i].add_new,testo[i].l,testo[i].range,1);
    }
    std::cout<<"1hop_time: "<<sumans<<std::endl;
    std::cout<<std::endl;
  }
  std::cout<<std::endl;

  // std::cout<<"----5hop----"<<std::endl;
  // T=1;
  // while(T--)
  // {
  //   double sumans=0;
  //   for(int i=1;i<=5;i++)
  //   {
  //     sumans+=k_top(db,testo[i].add_new,testo[i].l,testo[i].range,5);
  //   }
  //   //sumans+=k_top(db,testo[102].add_new,testo[102].l,testo[102].range,2);
  //   //sumans+=k_top(db,testo[108].add_new,testo[108].l,testo[108].range,2);
  //   std::cout<<"5hop_time: "<<sumans<<std::endl;
  //   std::cout<<std::endl;
  // }
  std::cout<<std::endl;
  return ans;
}
void solve_usdt()
{
  std::ifstream readFile;
  readFile.open("/home/z/test_data/5hop.txt",std::ios::in);
  int x=0;
  if (readFile.is_open())
	{
    std::string str,l,r;
    int64_t ll,rr;
		while (getline(readFile,str))
		{
      ++x;
      std::string from=str.substr(2,40);
      int pos1=42;
      int pos2=str.find("-");
      int pos3=str.find(":");
      l=str.substr(pos1+1,pos2-pos1);
      r=str.substr(pos2+1,pos3-pos2);
      ll=std::atoi(l.c_str());
      rr=std::atoi(r.c_str());
      int64_t add=static_cast<int64_t>(leveldb::Hash(from.c_str(),from.size(),0xbc9f1d34));
      std::string from_address="";
      from_address.append(8,'0');
      memcpy(&from_address[0],reinterpret_cast<void*>(&add),sizeof(int64_t));
      //std::cout<<"0x"<<from<<" "<<ll<<" to "<<rr<<std::endl;
      testo[x]=Testo(from,from_address,ll,rr,1);
    }
  }
  readFile.close();

  tq=0;
  int qt=0;
  readFile.open("/eth_data/usdt/modified_data/new_accounts.txt",std::ios::in);
  if (readFile.is_open())
	{
    std::string str,l,r;
    int64_t ll,rr;
    while (getline(readFile,str))
    {
      std::string from=str.substr(2,40);
      int64_t add=static_cast<int64_t>(leveldb::Hash(from.c_str(),from.size(),0xbc9f1d34));
      std::string from_address="";
      from_address.append(8,'0');
      memcpy(&from_address[0],reinterpret_cast<void*>(&add),sizeof(int64_t));
      // qt++;
      // if(qt>=10000000)
      //   testq[++tq]=Testo(from,from_address,15867322,16662284,1);
      address_f[from_address]="0x"+from;
     
    }
  }
}
int edn=0;
std::vector<int64_t> addnum[20005];
namespace boost{
void throw_exception(std::exception const & ex)
{
  std::cout<<"error"<<std::endl;
}
}

void eth_dbTest(){
  // leveldb::Env* env=leveldb::Env::Default();
  std::cout << "open database" << std::endl;
  leveldb::DB* db=nullptr;
  leveldb::Options opts;
  //opts.block_cache=leveldb::NewLRUCache(1000*1048576);
  opts.create_if_missing = true;
  leveldb::Status s=leveldb::DB::Open(opts,"/tmp/ramboDBtest_base",&db);
  leveldb::WriteBatch batch1;
  //return ;
  std::string db_state;

  std::cout << "read test data" << std::endl;
  solve_usdt();

  std::ifstream readFile;
  int64_t nownumber;
  int64_t sum=0,sum_insert=0;
  double sum_time=0,batch_time=0;
  cnt=0;

  double q;
  std::cout << "start test" << std::endl;
  // q=rating_test(db,1,1);

  
  //开始监听
  boost::asio::io_service io_service;
  // 创建一个TCP endpoint并绑定到本地8010端口
  tcp::endpoint endpoint(tcp::v4(), 8030);
  // 创建一个TCP acceptor并开始监听连接
  tcp::acceptor acceptor(io_service, endpoint);
  std::cout << "Listening on port 8030..." << std::endl;

  while (true) {
    // 等待连接
    tcp::socket socket(io_service);
    acceptor.accept(socket);

    // 读取HTTP请求
    boost::asio::streambuf request;
    boost::asio::read_until(socket, request, "\r\n\r\n");
    std::string request_string(boost::asio::buffers_begin(request.data()), boost::asio::buffers_end(request.data()));

    std::cout << "==========request==========" << std::endl;
    std::cout << request_string << std::endl;
    std::cout << "=========================" << std::endl;
    // 提取第一行
    std::size_t pos = request_string.find("\n");
    std::string first_line = request_string.substr(0, pos);
    // 解析请求参数
    std::unordered_map<std::string, std::string> params = parse_params(first_line);
    // 输出解析结果
    std::string query_type,address,contract;
    int64_t start,end,step;
    std::cout << "==========decoded data==========" << std::endl;
    for (const auto& kv : params) {
        std::cout << kv.first << ": " << kv.second << std::endl;
        if (kv.first == "query_type"){
            query_type = kv.second;
        }
        if(kv.first=="start_blk")
        {
          start=std::atoi(kv.second.c_str());
        }else if(kv.first=="end_blk")
        {
          end=std::atoi(kv.second.c_str());
        }else if(kv.first=="khop")
        {
          step=std::atoi(kv.second.c_str());
        }else if(kv.first=="address")
        {
          std::string tmp_add = kv.second.substr(2,40);
          int64_t add=static_cast<int64_t>(leveldb::Hash(tmp_add.c_str(),tmp_add.size(),0xbc9f1d34));
          address.append(8,'0');
          memcpy(&address[0],reinterpret_cast<void*>(&add),sizeof(int64_t));
        }
    }
    std::cout << "=========================" << std::endl;

    double elapsed_time = k_top(db,address,start,end,step);

    // 构造HTTP响应
    std::string response_json = get_edges_and_nodes();
    std::cout<<response_json<<std::endl;
    std::string response_string = "HTTP/1.1 200 OK\r\nContent-Length: " + std::to_string(response_json.length()) + "\r\n\r\n" + response_json;
    boost::asio::write(socket, boost::asio::buffer(response_string));
  }
  

  delete db;
}

void rambo_test(){
  for(int d=0;d<=1000;d++)
  {
    std::shared_ptr<RAMBO> filter(new RAMBO(150000,3,5,100,0));
    for(int i=1;i<=30;i++)
    {
      filter->createMetaRambo_single(d*30+i,i);
    }
    filter->out_set();
  }
  
}

int main(int argc, char** argv) {
  eth_dbTest();
  return 0;
}
