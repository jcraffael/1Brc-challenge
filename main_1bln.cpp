#include <iostream>
#include <string>
#include <vector>
#include <cstring>
#include <numeric>
#include <assert.h>
#include <fstream>
#include <functional>
#include <map>
#include <math.h>
#include <sstream>
#include <fcntl.h>
#include <sys/stat.h>
#include <future>
#include <thread>
#include <sys/types.h>
#include <sys/mman.h>
#include <unistd.h>
#include <mutex>
#include <chrono>


struct dataPerCity
{
	float min;
	float max;
	float av;
	uint32_t num;
	dataPerCity():min(-100.0), max(100.0), av(0), num(0){}
	void newTemp(float t)
	{
		av = (av * num + t) / (num + 1);
		num++;
		if(min == -100.0 || t < min) min = t;
		if(max == 100.0 || t > max) max = t;
	}
	void newData(dataPerCity &dp)
	{
		if(dp.min < min) min = dp.min;
		if(dp.max > max) max = dp.max;
		av = (av * num + dp.av * dp.num) / (num + dp.num);
		num += dp.num;
	}
	
};

typedef std::unordered_map<std::string, dataPerCity> dataSet;

dataSet parse(std::string str)
{
	dataSet newDataSet;
	//cout << "line 138, th str is " << str << endl;
	/*
	char *token = strtok(str,"\n");
	while(token != nullptr)
	{
		cout << "Token is " << token << endl;
		string tk(token);
		dataPerCity dc;
		string city = tk.substr(0, tk.find(";"));
		dc.newTemp(stod(tk.substr(tk.find(";") + 1)));
		
		cout << "city is " << city << " and temp is " << dc.av << endl;
		newDataSet.insert(make_pair(city, dc));
		token = strtok(NULL, "\n");
	}*/
	std::string tmp;
	std::stringstream ss(str);
	while(std::getline(ss, tmp))
	{
		
		std::string city = tmp.substr(0, tmp.find(";"));
		
		if(newDataSet.find(city) == newDataSet.end())
		{
			dataPerCity dc;
			dc.newTemp(std::stof(tmp.substr(tmp.find(";") + 1)));
			newDataSet.insert(std::make_pair(city, dc));
		}	
		else
		{
			newDataSet[city].newTemp(std::stof(tmp.substr(tmp.find(";") + 1)));
		}
	}
	
	return newDataSet;
}

class fondation
{
	int fd;
	uint64_t lines = 1000000000;
	uint64_t core;
	std::vector<std::future<dataSet>> vecFut;
	std::map<std::string, dataPerCity> finalCollect;
	char *ptr = nullptr;
	size_t mapSize = 0;
	struct stat statbuf;
	std::mutex m;
public:
	fondation() = delete;
	fondation(const fondation &) = delete;
	fondation& operator=(const fondation&) = delete;
	fondation(char *filePath)
	{
		int fd = open(filePath, O_RDWR);
		if(fd < 0){
			printf("\n\"%s \" could not open\n",filePath);
			exit(1);
		}

		int err = fstat(fd, &statbuf);
		if(err < 0){
			 printf("\n\"%s \" could not open\n",filePath);
			 exit(2);
		}
		mapSize = statbuf.st_size;
		ptr = static_cast<char*>(mmap(NULL, mapSize,
					 PROT_READ, MAP_SHARED, fd, 0));
		if(ptr == MAP_FAILED){
			printf("Mapping Failed\n");
			exit(1);
		}
		
		core = std::thread::hardware_concurrency();
	}
	
	void fileProcess()
	{
		int size = lines/core;
		char *start = ptr;
		//cout << "Size is " << size << " and total len is " << statbuf.st_size << endl;
		int i = 0;
		while(start < ptr + statbuf.st_size)
		{
			std::lock_guard<std::mutex> lg(m);

			if(start + size <= ptr + statbuf.st_size)
			{
				while(start[size] != '\n' && start[size] !=0x0)
					size++;
			}
			else
				size = ptr + statbuf.st_size - start;

			std::string str;
			str.assign(start, start+size);
			size++;
			std::future<dataSet> f = std::async(std::launch::async, parse, str);
			vecFut.push_back(std::move(f));
			start += size;
		}
	}
	
	void dataCollection()
	{
		for(auto &f : vecFut)
		{
			dataSet dS = f.get();
			auto iter = dS.begin();
			while(iter != dS.end())
			{
				//cout << iter->first << ": " << iter->second.av << ", with num " << iter->second.num <<endl;
				if(finalCollect.find(iter->first) == finalCollect.end())
				{
					finalCollect.insert(*iter);
				}
				else
				{
					finalCollect[iter->first].newData(iter->second);
				}
				iter++;
			}
		}
	}
	
	void printRes()
	{
		for(auto &f : finalCollect)
		{
			
			printf("%s:%.1f/%.1f/%.1f\n",f.first.c_str(), f.second.min, f.second.av, f.second.max);
		}
	}
	~fondation()
	{
		if (fd >= 0) { close(fd); }
		if(ptr) {munmap(ptr, statbuf.st_size);}
	}
};



int main(int argc, char *argv[]){

	auto tStart = std::chrono::steady_clock::now();
    if(argc < 2){
        printf("File path not mentioned\n");
        exit(0);
    }

    fondation fd(argv[1]);
	fd.fileProcess();
	fd.dataCollection();
	fd.printRes();
	
	auto tEnd = std::chrono::steady_clock::now();
	std::cout << "The time is " << std::chrono::duration_cast<std::chrono::duration<double>>(tEnd - tStart).count() << std::endl;

	return 0;
}

