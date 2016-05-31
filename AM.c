#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "AM.h"
#include "BF.h"

size_t attrT1,attrL1,attrT2,attrL2,rtid,lftid,intsize;
int AM_errno;
typedef struct{
    int am_file_pos;    //the position of the (open) index in AM_files array
    int current_block_id;
    int scan_type;
    int record_n;    //number of the (previous) record in the block. -1 if scan must begin from the block's 1st record
    char *search_value;
    char *tempbuffer;
}index_scan_t;

typedef  struct{
    int id,rootid,lefterid; //BF ID , id of the root block , id of the most left data block
    char name[50];          //name of the file
    char attrType1,attrType2;
   size_t attrLength1,attrLength2;
}AM_File_Info;


int insert(int filed,int blockid,void *value1,void *value2,int *newid,void **value,int(*beq)(int,void*,void*) ,int fileDesc);
int beq(int fileDesc,void *value1,void *value2);
int compare(int fileDesc, void *value1, void *value2);

    AM_File_Info emptyFile;
AM_File_Info AM_files[MAXOPENFILES];
index_scan_t index_scans[MAXSCANS];
int number_of_opened_files=0;

void AM_Init( void ){
    BF_Init();
    AM_errno = AME_OK;
    int i;
    intsize=sizeof(AMFILE);
    attrT1=sizeof(AMFILE)+1;
    attrL1=attrT1+1;
    attrT2=attrL1+sizeof(int);
    attrL2=attrT2+1;
    rtid=attrL2+sizeof(int);
    lftid=rtid+sizeof(int);
    for(i=0;i<MAXOPENFILES;i++){
        AM_files[i].id=-1;
    }
    for (i = 0 ; i < MAXSCANS ; ++i) index_scans[i].am_file_pos= -1;
}

int AM_CreateIndex( char *fileName, char attrType1 ,int attrLength1 ,char attrType2 ,int attrLength2){
    int n=12;
    //checking for correct info
    if(((attrType1=='i'||attrType1=='f')&&attrLength1!=4)||((attrType2=='i'||attrType2=='f')&&attrLength2!=4)){
        AM_errno=AME_FILE_CR_FAIL;
        return AME_FILE_CR_FAIL;
    }
    if((attrType1=='c' && (attrLength1<=0 || attrLength1 >256 ))|| (attrType2=='c'&&(attrLength2<=0 || attrLength2>256))){
        AM_errno=AME_FILE_CR_FAIL;
        return AME_FILE_CR_FAIL;
    }
    //checking if file exists
    if((n=BF_OpenFile(fileName))==0){
        if(BF_CloseFile(n)<0){
            AM_errno = AME_CLOSE_FILE_FAILED;
            return AME_CLOSE_FILE_FAILED;
        }
        AM_errno=AME_FILE_CR_FAIL;
        return AME_FILE_CR_FAIL;
    }
    else{
        void * block2write;
        int filenumber;
        if(BF_CreateFile(fileName)<0){
            AM_errno=AME_CREATE_FILE_FAILED;
            return AME_CREATE_FILE_FAILED;
        }
        if((filenumber=BF_OpenFile(fileName))<0){
            AM_errno=AME_OPEN_FILE_FAILED;
            return AME_OPEN_FILE_FAILED;
        }
        if(BF_AllocateBlock(filenumber)<0){
            AM_errno=AME_ALLOCATE_BLOCK_FAILED;
            return AME_ALLOCATE_BLOCK_FAILED;
        }
        if(BF_ReadBlock(filenumber,0,&block2write)<0){
            AM_errno=AME_FILE_READ_FAILED;
            return AME_FILE_READ_FAILED;
        }
        //Writing info at the info block
        strcpy(block2write,AMFILE);
        memcpy(block2write+attrT1,&attrType1,1);
        memcpy(block2write+attrL1,&attrLength1,sizeof(int));
        memcpy(block2write+attrT2,&attrType2,1);
        /* printf("%c,%c\n",attrType2,*(char*)(block2write+attrT2)); */
        memcpy(block2write+attrL2,&attrLength2,sizeof(int));
       * ((int *) (block2write+rtid))=-1; //rootid
       * ((int *) (block2write+lftid))=-1; //lefter data block id
       *(char*)(block2write+intsize)=sizeof(int);
        if(BF_WriteBlock(filenumber,0)<0){
            AM_errno=AME_WRITE_BLOCK_FAILED;
            return AME_WRITE_BLOCK_FAILED;
        }
        if(BF_CloseFile(filenumber)<0){
            AM_errno=AME_CLOSE_FILE_FAILED;
            return AME_CLOSE_FILE_FAILED;
        }
    }
    return AME_OK;
}

int AM_OpenIndex(char* fileName){
    int filenumber,amfilenumber = -1,i;
    void *firstblock;
    if(number_of_opened_files==MAXOPENFILES){
        AM_errno = AME_FILE_OP_FAIL;
        return AME_FILE_OP_FAIL;
    }
    if((filenumber=BF_OpenFile(fileName))<0){
        AM_errno = AME_OPEN_FILE_FAILED;
        return AME_OPEN_FILE_FAILED;
    }
    if(BF_ReadBlock(filenumber,0,&firstblock)<0){
        AM_errno = AME_FILE_READ_FAILED;
        return AME_FILE_READ_FAILED;
    }
    //checking if file is a Block file of this application
    if(strcmp(firstblock,AMFILE)!=0){
        AM_errno=AME_FILE_OP_FAIL;
        return AME_FILE_OP_FAIL;
    }
    //checking if sizeof(int) matches the one in the block
    if(*(char*)(firstblock+intsize)!=sizeof(int)){
        AM_errno=AME_FILE_OP_FAIL;
        return AME_FILE_OP_FAIL;
    }
    //find an empty position
    for(i=0;i<MAXOPENFILES;i++){
        if(AM_files[i].id<0){
            amfilenumber=i;
            break;
        }
    }
    if (amfilenumber == -1)
    {
        AM_errno = AME_NO_SPACE;
        return AME_NO_SPACE;
    }
    //copying data in the struct array of AM_File_Info
    AM_files[amfilenumber].id=filenumber;
    strcpy(AM_files[amfilenumber].name,fileName);
    memcpy(&AM_files[amfilenumber].attrType1,firstblock+attrT1,1);
    memcpy(&AM_files[amfilenumber].attrLength1,firstblock+attrL1,sizeof(int));
    memcpy(&AM_files[amfilenumber].attrType2,firstblock+attrT2,1);
    memcpy(&AM_files[amfilenumber].attrLength2,firstblock+attrL2,sizeof(int));
    AM_files[amfilenumber].rootid=*(int*)(firstblock+rtid);
    AM_files[amfilenumber].lefterid=*(int*)(firstblock+lftid);
    number_of_opened_files++;
    /* printf("%c,%c\n",AM_files[amfilenumber].attrType1,AM_files[amfilenumber].attrType2); */
    return amfilenumber;
}

int AM_DestroyIndex(char *fileName){
    int i,n;
    //checking if file is open
    for(i=0;i<MAXOPENFILES;i++){
        if(strcmp(AM_files[i].name,fileName)==0 && AM_files[i].id!=-1){
            AM_errno=AME_DESTR_FAIL;
            return AME_DESTR_FAIL;
        }
    }
    if((n=BF_OpenFile(fileName))<0){
        AM_errno=AME_OPEN_FILE_FAILED;
        return AME_OPEN_FILE_FAILED;
    }
    if(BF_CloseFile(n)<0){
        AM_errno=AME_CLOSE_FILE_FAILED;
        return AME_CLOSE_FILE_FAILED;
    }
    remove(fileName);
    return AME_OK;
}

int AM_CloseIndex(int fileDesc){
    int i;
    void * block2write;
    //check for open scans
    for (i = 0 ; i < MAXSCANS ; ++i)
    {
        if (index_scans[i].am_file_pos == fileDesc)
        {
            AM_errno = AME_SCAN_OPEN;
            return AME_SCAN_OPEN;
        }
    }
    if(fileDesc>=MAXOPENFILES || fileDesc <0){
        AM_errno=AME_CLOSE_FAIL;
        return AME_CLOSE_FAIL;
    }
    if(BF_ReadBlock(AM_files[fileDesc].id,0,&block2write)<0){
        AM_errno=AME_FILE_READ_FAILED;
        return AME_FILE_READ_FAILED;
    }
    //writing new data in the info block
    * (int *) (block2write+rtid)=AM_files[fileDesc].rootid;
    * (int *) (block2write+lftid)=AM_files[fileDesc].lefterid;
    if(BF_WriteBlock(AM_files[fileDesc].id,0)<0){
        AM_errno=AME_WRITE_BLOCK_FAILED;
        return AME_WRITE_BLOCK_FAILED;
    }
    if(BF_CloseFile(AM_files[fileDesc].id)<0){
        AM_errno=AME_CLOSE_FILE_FAILED;
        return AME_CLOSE_FILE_FAILED;
    }
    AM_files[fileDesc].id=-1;
    number_of_opened_files--;
    return AME_OK;
}

int split(void *b1,void *b2, void *val,void *val2,int id,int (*beq)(int ,void *,void *),int fileDesc){
    size_t offset,offset2;
    int counter,i,num_of_tuples;
    void* temp;
    size_t s,s2;
    s=AM_files[fileDesc].attrLength1;
    s2=AM_files[fileDesc].attrLength2;
    if((*(char*)b1)=='b'){
        counter=*((int*)(b1+1));
        num_of_tuples=(counter+1)/2;
        offset=1+2*sizeof(int);
        temp=malloc((counter+1)*(s)+(counter+2)*sizeof(int));
        memmove(temp,b1+offset-sizeof(int),(counter)*(s)+(counter+1)*sizeof(int));
        offset2=sizeof(int);
        for(i=0;i<counter;i++){
            if(!beq(fileDesc,val,temp+offset2)){
                break;
            }
            else{
                offset2+=s+sizeof(int);
            }
        }
        //creating space in temp for the pair val,id
        memmove(temp+offset2+s+sizeof(int),temp+offset2,(s+sizeof(int))*(counter-i));
        memcpy(temp+offset2,val,s);
        *(int*)(temp+offset2+s)=id;
        offset2=sizeof(int);
        offset=1+2*sizeof(int);
        //copying values in the new/old block
        memmove(b1+offset-sizeof(int),temp,num_of_tuples*s+(num_of_tuples+1)*sizeof(int));
        *(int*)(b1+1)=num_of_tuples;
        memmove(b2+offset-sizeof(int),temp+(num_of_tuples+1)*(s+sizeof(int)),(num_of_tuples-((counter)%2))*s+(num_of_tuples-(((counter)%2)*1)+1)*sizeof(int));
        *(int*)(b2+1)=num_of_tuples-((counter)%2)*1;
        memcpy(val,b2+offset,s);
        free(temp);
    }
    else if((*(char*)b1)=='d'){
        counter=*((int*)(b1+1+sizeof(int)));
        num_of_tuples=(counter+1)/2;
        offset=1+2*sizeof(int);
        temp=malloc((counter+1)*(s+s2));
        memmove(temp,b1+offset,(counter)*(s+s2));
        offset2=0;
        for(i=0;i<counter;i++){
            if(!beq(fileDesc,val,temp+offset2)){
                break;
            }
            else{
                offset2+=s+s2;
            }
        }
        memmove(temp+offset2+s+s2,temp+offset2,(s+s2)*(counter-i));
        memcpy(temp+offset2,val,s);
        memcpy(temp+offset2+s,val2,s2);
        //checking if splitting happens between two equal values
        if(compare(fileDesc,temp+((num_of_tuples-1)*(s+s2)),temp+(num_of_tuples*(s+s2)))!=0){
            memmove(b1+offset,temp,num_of_tuples*(s+s2));
            *(int*)(b1+1+sizeof(int))=num_of_tuples;
            memmove(b2+offset,temp+((num_of_tuples)*(s+s2)),(num_of_tuples+((counter+1)%2))*(s+s2));
            *(int*)(b2+1+sizeof(int))=num_of_tuples+((counter+1)%2);
        }
        else{

            /* if(AM_files[fileDesc].attrType1=='i'&&AM_files[fileDesc].attrType2=='c'){ */
            /*     printf("%d,%d\n",*(int*)(temp+(num_of_tuples-1)*(s+s2)),*(int*)(temp+(num_of_tuples*(s+s2)))); */
            /* } */
            /* size_t printoffset=0; */
            /* for(i=0;i<counter+1;i++){ */
            /*     if(AM_files[fileDesc].attrType1=='i'&&AM_files[fileDesc].attrType2=='c'){ */
            /*         printf("|%d %s ",*(int*)(temp+printoffset),(char*)(temp+printoffset+sizeof(int))); */
            /*     } */
            /*     printoffset+=s+s2; */
            /* } */
            /* printf("\n"); */
            size_t offset3=0;
            int newnumb=num_of_tuples+1;
            //if equal values start from the begining of the block then they stay in block 1
            if(compare(fileDesc,temp,temp+num_of_tuples*(s+s2))==0){
                while(compare(fileDesc,temp+((num_of_tuples)*(s+s2)),temp+((newnumb)*(s+s2)))==0){
                    newnumb++;
                    if(newnumb==counter+1){
                        printf("Error cant split block. Too many Values are equal\n");
                        return AME_INSERT_FAIL;
                    }
                }
                num_of_tuples=newnumb;
                memmove(b1+offset,temp,num_of_tuples*(s+s2));
                *(int*)(b1+1+sizeof(int))=num_of_tuples;
                memmove(b2+offset,temp+((num_of_tuples)*(s+s2)),((counter+1)-num_of_tuples)*(s+s2));
                *(int*)(b2+1+sizeof(int))=(counter+1)-num_of_tuples;
            }
            //else equal values get in the new block
            else{
                int newnumb=0;
                while(compare(fileDesc,temp+offset3,temp+(num_of_tuples*(s+s2)))!=0){
                    offset3+=s+s2;
                    newnumb++;
                }
                num_of_tuples=newnumb;
                memmove(b1+offset,temp,num_of_tuples*(s+s2));
                *(int*)(b1+1+sizeof(int))=num_of_tuples;
                memmove(b2+offset,temp+((num_of_tuples)*(s+s2)),((counter+1)-num_of_tuples)*(s+s2));
                *(int*)(b2+1+sizeof(int))=(counter+1)-num_of_tuples;
            }
            /* printoffset=1+2*sizeof(int); */
            /* for(i=0;i<num_of_tuples;i++){ */
            /*     if(AM_files[fileDesc].attrType1=='i'&&AM_files[fileDesc].attrType2=='c'){ */
            /*         printf("|%d ,%s ",*(int*)(b1+printoffset),(char*)(b1+printoffset+sizeof(int))); */
            /*     } */
            /*     printoffset+=s+s2; */
            /* } */
            /* printf("\n"); */
            /* printoffset=1+2*sizeof(int); */
            /* for(i=0;i<counter+1-num_of_tuples;i++){ */
            /*     if(AM_files[fileDesc].attrType1=='i'&&AM_files[fileDesc].attrType2=='c'){ */
            /*         printf("|%d ,%s ",*(int*)(b2+printoffset),(char*)(b2+printoffset+sizeof(int))); */
            /*     } */
            /*     printoffset+=s+s2; */
            /* } */
            /* printf("\n"); */
        }
        free(temp);
    }
    return 0;
}

int insert(int filed,int blockid,void *value1,void *value2,int *newid,void **value,int (* beq)(int,void *,void *),int fileDesc){
    void * block,*newblock;
    int counter,i,blockid_down,error;
    size_t offset;
    AM_File_Info *info;
    info=&AM_files[fileDesc];
    if(BF_ReadBlock(filed,blockid,&block)<0){
        AM_errno=AME_FILE_READ_FAILED;
        return AME_FILE_READ_FAILED;
    }
    /* printf("called with blockid %d\n",blockid); */
    //if block is not a leaf
    if (*((char*)block)=='b'){
        counter=*(int *)(block+1);
        offset=1+2*sizeof(int);
        /* for(i=0;i<counter;i++){ */
            /* if(info->attrType1=='c'){ */
                /* printf("|%d| %s ",*(int*)(block+offset-sizeof(int)),(char*)(block+offset)); */
            /* } */
            /* offset+=info->attrLength1+sizeof(int); */
        /* } */
        /* offset=1+2*sizeof(int); */
        /* printf("\n"); */
        for(i=0;i<counter;i++){
            if(!beq(fileDesc,value1,block+offset)){
                blockid_down=*(int*)(block+offset-sizeof(int));
                break;
            }
            else{
                offset+=info->attrLength1+sizeof(int);
                blockid_down=*(int*)(block+offset-sizeof(int));
            }
        }
        //recursive call of insert
        if((error=insert(filed,blockid_down,value1,value2,newid,value,beq,fileDesc)<0)){
                return error;
        }
        /* printf("Again in %d, newid=%d\n",blockid,*newid); */
        if(*newid==-1){
            return AME_OK; //no new value to add exit
        }
        else{//new value to addi
            if((BLOCK_SIZE-1-sizeof(int)-(counter*(info->attrLength1))-(counter+1)*sizeof(int))>=info->attrLength1+sizeof(int)){
                offset=1+2*sizeof(int);
                //finding the correct place for the new value
                for(i=0;i<counter;i++){
                    if(!beq(fileDesc,*value,block+offset)){
                        break;
                    }
                    else{
                        offset+=info->attrLength1+sizeof(int);
                    }
                }
               //moving keys and pointers(ints) to create space for the new value and pointer
                memmove(block+offset+sizeof(int)+info->attrLength1,block+offset,(info->attrLength1+sizeof(int))*(counter-i));
                memcpy(block+offset,*value,info->attrLength1);
                *(int*)(block+offset+info->attrLength1)=*newid;
                (*(int *)(block+1))++;
                offset=1+2*sizeof(int);
                *newid=-1;
                if(BF_WriteBlock(filed,blockid)<0){
                    AM_errno=AME_WRITE_BLOCK_FAILED;
                    return AME_WRITE_BLOCK_FAILED;
                }
                return AME_OK;
            }
            else{
                /* printf("NO SPACE HERE\n"); */
                int id=*newid;//id received from below
                if((BF_AllocateBlock(filed))<0){
                    AM_errno = AME_ALLOCATE_BLOCK_FAILED;
                    return AME_ALLOCATE_BLOCK_FAILED;
                }
                if((*newid=BF_GetBlockCounter(filed)-1)<0){// id of the block to send in the above level
                    AM_errno=AME_GET_BLOCK_C_FAILED;
                    return AME_GET_BLOCK_C_FAILED;
                }
                if(BF_ReadBlock(filed,*newid,&newblock)<0){
                    AM_errno=AME_FILE_READ_FAILED;
                    return AME_FILE_READ_FAILED;
                }
                *(char*)newblock='b';
                /* splitting here */
                split(block,newblock,*value,NULL,id,beq,fileDesc);//!!!!!! split changes value to the new value to add in the above level
                if(BF_WriteBlock(filed,blockid)<0){
                    AM_errno=AME_WRITE_BLOCK_FAILED;
                    return AME_WRITE_BLOCK_FAILED;
                }
                if(BF_WriteBlock(filed,*newid)<0){
                    AM_errno=AME_WRITE_BLOCK_FAILED;
                    return AME_WRITE_BLOCK_FAILED;
                }
                if(blockid==info->rootid){//if in root node make new root!
                    int rootid;
                    void * rootblock;
                    if((BF_AllocateBlock(filed))<0){
                        AM_errno = AME_ALLOCATE_BLOCK_FAILED;
                        return AME_ALLOCATE_BLOCK_FAILED;
                    }
                    if((rootid=BF_GetBlockCounter(filed)-1)<0){
                        AM_errno=AME_GET_BLOCK_C_FAILED;
                        return AME_GET_BLOCK_C_FAILED;
                    }
                    /* printf("new root created with id %d",rootid); */
                    BF_ReadBlock(filed,rootid,&rootblock);
                    *(char*)rootblock='b';
                    *(int *)(rootblock +1)=1;
                    *(int *)(rootblock +1+sizeof(int))=blockid;
                    *(int *)(rootblock +1+2*sizeof(int)+info->attrLength1)=id;
                    memcpy(rootblock+1+sizeof(int)+sizeof(int),*value,info->attrLength1);
                    info->rootid=rootid;//changing the rootid in AM_files table
                    if(BF_WriteBlock(filed,rootid)<0){
                        AM_errno=AME_WRITE_BLOCK_FAILED;
                        return AME_WRITE_BLOCK_FAILED;
                    }
                    return AME_OK;
                }
            }
        }
    }
    else if (*(char*) block== 'd'){
        //if there is space
        counter=*(int *)(block+1+sizeof(int));
        /* offset=1+2*sizeof(int); */
        /* for(i=0;i<counter;i++){ */
        /*     if(info->attrType1=='i'&&info->attrType2=='c'){ */
        /*         printf("|%d, %s ",*(int*)(block+offset),(char*)(block+offset+sizeof(int))); */
        /*     } */
        /*     offset+=info->attrLength1+info->attrLength2; */
        /* } */
        /* printf("\n"); */
        offset=1+2*sizeof(int);
        //if there is space in the data block
        if((BLOCK_SIZE-(1+sizeof(int)+sizeof(int)+counter*(info->attrLength1+info->attrLength2)))>=(info->attrLength1+info->attrLength2)){//if there is space
            for(i=0;i<counter;i++){
                if(!beq(fileDesc,value1,block+offset)){
                    break;
                }
                else {
                   offset+=info->attrLength2+info->attrLength1;
                }
            }
            memmove(block+offset+info->attrLength1+info->attrLength2,block+offset,(counter-i)*(info->attrLength1+info->attrLength2));
            memmove(block+offset,value1,info->attrLength1);
            memmove(block+offset+info->attrLength1,value2,info->attrLength2);
            (*(int *)(block+1+sizeof(int)))++;
            *newid=-1;
            if(BF_WriteBlock(filed,blockid)<0){
                AM_errno=AME_WRITE_BLOCK_FAILED;
                return AME_WRITE_BLOCK_FAILED;
            }
            return AME_OK;
        }
        //else split the block
        else{
            int newblockid;
            void *newblock;
            if(BF_AllocateBlock(filed)<0){
                AM_errno = AME_ALLOCATE_BLOCK_FAILED;
                return AME_ALLOCATE_BLOCK_FAILED;
            }
            if((newblockid=BF_GetBlockCounter(filed)-1)<0){
                AM_errno=AME_GET_BLOCK_C_FAILED;
                return AME_GET_BLOCK_C_FAILED;
            }
            if(BF_ReadBlock(filed,newblockid,&newblock)){
                AM_errno=AME_FILE_READ_FAILED;
                return AME_FILE_READ_FAILED;
            }
            *(char*)newblock='d';
            *(int*)(newblock+1)=*(int *)(block+1);
            *(int*)(block+1)=newblockid;
            int nothing=0;
            if(split(block,newblock,value1,value2,nothing,beq,fileDesc)<0){
                return AME_INSERT_FAIL;
            }
            if(BF_WriteBlock(filed,blockid)<0){
                AM_errno=AME_WRITE_BLOCK_FAILED;
                return AME_WRITE_BLOCK_FAILED;
            }
            if(BF_WriteBlock(filed,newblockid)<0){
                AM_errno=AME_WRITE_BLOCK_FAILED;
                return AME_WRITE_BLOCK_FAILED;
            }
            memcpy(*value,newblock+1+2*sizeof(int),info->attrLength1);
            *newid=newblockid;
            //if only one block create an index block (root)
            if(blockid==info->rootid){
                int rootid;
                void *rootblock;
                if((BF_AllocateBlock(filed))<0){
                    AM_errno = AME_ALLOCATE_BLOCK_FAILED;
                    return AME_ALLOCATE_BLOCK_FAILED;
                }
                if((rootid=BF_GetBlockCounter(filed)-1)<0){
                    AM_errno= AME_GET_BLOCK_C_FAILED;
                    return AME_GET_BLOCK_C_FAILED;
                }
                if(BF_ReadBlock(filed,rootid,&rootblock)<0){
                    AM_errno=AME_FILE_READ_FAILED;
                    return AME_FILE_READ_FAILED;
                }
                *(char*)rootblock='b';
                *(int *)(rootblock +1)=1;
                *(int *)(rootblock +1+sizeof(int))=blockid;
                *(int *)(rootblock +1+2*sizeof(int)+info->attrLength1)=newblockid;
                memcpy(rootblock+1+sizeof(int)+sizeof(int),newblock+1+2*sizeof(int),info->attrLength1);
                /* printf("new root created with id %d",rootid); */
                AM_files[fileDesc].rootid=rootid;
                info->lefterid=blockid;
                if(BF_WriteBlock(filed,rootid)<0){
                    AM_errno=AME_WRITE_BLOCK_FAILED;
                    return AME_WRITE_BLOCK_FAILED;
                }
                return AME_OK;
            }
            return AME_OK;
        }
    }
    return AME_OK;
}

int AM_InsertEntry(int fileDesc, void *value1, void *value2)
{
    int blockid,ret;
    void* block;
    //if there is no root created for this index
    if(AM_files[fileDesc].rootid<0){
        if(BF_AllocateBlock(AM_files[fileDesc].id)<0){
            AM_errno = AME_ALLOCATE_BLOCK_FAILED;
            return AME_ALLOCATE_BLOCK_FAILED;
        }
        if((blockid=BF_GetBlockCounter(AM_files[fileDesc].id)-1)<0){
            AM_errno=AME_GET_BLOCK_C_FAILED;
            return AME_GET_BLOCK_C_FAILED;
        }
        AM_files[fileDesc].rootid=blockid;
        if(BF_ReadBlock(AM_files[fileDesc].id,blockid,&block)<0){
            AM_errno = AME_FILE_READ_FAILED;
            return AME_FILE_READ_FAILED;
        }
        *(char*)block='d';
        *(int*)(block+sizeof(int)+1)=0;
        *(int*)(block+1)=-1;
    }
    void *value;
    int id=-1;
    value=malloc(AM_files[fileDesc].attrLength1);
    /* printf("INSERT CALL\n"); */
    /* if(AM_files[fileDesc].attrType1=='c'){ */
        /* printf("value to add %s\n",(char*)value1 ); */
    /* } */
    ret=insert(AM_files[fileDesc].id,AM_files[fileDesc].rootid,value1,value2,&id,&value,beq,fileDesc);
    free(value);
    return ret;
}

int compare(int fileDesc, void *value1, void *value2)    //fileDesc: the position of the index in the AM_files array
 {//returns: > 0 if value1 > value2, < 0 if value1 < value2, 0 if value1 = value2 EOF for error
    if (AM_files[fileDesc].attrType1 == 'i')
    {
        if (*(int *)value1 == *(int *)value2) return 0;
        else if (*(int *)value1 > *(int *)value2) return 1;
        else if (*(int *)value1 < *(int *)value2) return -1;
    }
    else if (AM_files[fileDesc].attrType1 == 'f')
    {
        if (*(float *)value1 == *(float *)value2) return 0;
        else if (*(float *)value1 > *(float *)value2) return 1;
        else if (*(float *)value1 < *(float *)value2) return -1;
    }
    else //string
    {
        return strcmp((char *)value1, (char *)value2);
    }
    return EOF;
}

int beq(int fileDesc,void *value1,void *value2){
    return ((compare(fileDesc,value1,value2)==0)||(compare(fileDesc,value1,value2)>0));
}

int AM_OpenIndexScan(int fileDesc, int op, void *value)
{
    int i, position = -1, counter, next_block_id;
    char block_type;
    void *temp_block;
    if (AM_files[fileDesc].id < 0)
    {
        AM_errno = AME_FILE_NOT_OPEN;
        return AME_FILE_NOT_OPEN;
    }
    //find an empty position
    for (i = 0 ; i < MAXSCANS ; ++i)
        if (index_scans[i].am_file_pos < 0)
        {
            position = i;
            break;
        }
    if (position == -1)
    {
        AM_errno = AME_NO_SPACE;
        return AME_NO_SPACE;
    }
    index_scans[position].am_file_pos = fileDesc;
    index_scans[position].scan_type = op;
    //allocate memory for buffers
    if ( (index_scans[position].search_value = malloc(AM_files[fileDesc].attrLength1 * sizeof(char))) == NULL)
    {
        AM_errno = AME_MALLOC_FAILED;
        return AME_MALLOC_FAILED;
    }
    if ( (index_scans[position].tempbuffer = malloc((AM_files[fileDesc].attrLength2) * sizeof(char))) == NULL)
    {
        AM_errno = AME_MALLOC_FAILED;
        return AME_MALLOC_FAILED;
    }
    //copy value to search for
    memcpy((void *)(index_scans[position].search_value), value, AM_files[fileDesc].attrLength1);
    //search through the B+tree to find correct data block
    switch(op)
    {
        case EQUAL:    //in all following 3 cases, we must find the correct block and the record to begin searching
        case GREATER_THAN_OR_EQUAL:
        case GREATER_THAN:
            //read root block
            if (BF_ReadBlock(AM_files[fileDesc].id, AM_files[fileDesc].rootid, &temp_block) < 0){
                AM_errno = AME_FILE_READ_FAILED;
                return AME_FILE_READ_FAILED;
            }
            memcpy((void *)&block_type, temp_block, 1);
            //if it's a data block (all of the tree's data fit in 1 block, so there's not really a tree)
            if (block_type == 'd') index_scans[position].current_block_id = AM_files[fileDesc].rootid;
            //otherwise follow the tree to get to the correct data block
            else
            {
                while(block_type != 'd')
                {
                    //copy get the counter
                    counter = *(int *)(temp_block + 1);
                    //search through the keys to find the appropirate one to continue
                    i = 0;
                    while ((i < counter) && !(compare(fileDesc, (void *)temp_block + 1 + sizeof(int) + i*(sizeof(int) + AM_files[fileDesc].attrLength1) + sizeof(int), value) > 0) ) i++;
                    // | |-while the iteration counter (i) is within the limits of the number of index records, and we haven't found a key value that is greater than the search key, keep searching
                    // |--block_type = 1, counter is integer, pointer is integer (a block id) + length of key (attribute 1) = sizeof(int) + AM_files[fileDesc].attrLength1, + sizeof(int) to get the address of the attribute.
                    next_block_id = (char) *((int *)(temp_block + 1 + sizeof(int) + i*(sizeof(int) + AM_files[fileDesc].attrLength1)));
                    if (BF_ReadBlock(AM_files[fileDesc].id, next_block_id, &temp_block) < 0){
                        AM_errno = AME_FILE_READ_FAILED;
                        return AME_FILE_READ_FAILED;
                    }
                    memcpy((void *)&block_type, temp_block, 1);
                }
                index_scans[position].current_block_id = next_block_id;
            }
            //correct data block found, now the correct position of the record inside the block must be found
            if (BF_ReadBlock(AM_files[fileDesc].id, index_scans[position].current_block_id, &temp_block) < 0){
                AM_errno = AME_FILE_READ_FAILED;
                return AME_FILE_READ_FAILED;
            }
            counter = *(int *)(temp_block + 1 + sizeof(int)); //get the record counter
            /* size_t printoffset=1+2*sizeof(int); */
            /* for(i=0;i<counter+1;i++){ */
            /*     if(AM_files[fileDesc].attrType1=='i'&& AM_files[fileDesc].attrType2=='c'){ */
            /*         printf("|%d ,%s ",*(int*)(temp_block+printoffset),(char*)(temp_block+printoffset+AM_files[fileDesc].attrLength1)); */
            /*     } */
            /*     else */
            /*         printf("%c,%c\n",AM_files[fileDesc].attrType1,AM_files[fileDesc].attrType2); */
            /*     printoffset+=AM_files[fileDesc].attrLength1+AM_files[fileDesc].attrLength2; */
            /* } */
            /* printf("\n"); */
            i = 0;
            if (op == GREATER_THAN)    //if we need strictly greater, a different comparison must be used from the other cases (equal, geq)
                while ((i < counter) && !(compare(fileDesc, (void *)temp_block + 1 + 2*sizeof(int) + i*(AM_files[fileDesc].attrLength1 + AM_files[fileDesc].attrLength2), value) > 0) ) i++;
            else    //in both equal and geq, we begin searching from a record that has a key value equal to the search value
                while ((i < counter) &&  (compare(fileDesc, (void *)temp_block + 1 + 2*sizeof(int) + i*(AM_files[fileDesc].attrLength1 + AM_files[fileDesc].attrLength2), value) != 0) ) i++;
            index_scans[position].record_n = i - 1;    //-1 so the "next" (+ 1) record is the correct one
            break;
        case NOT_EQUAL://in all following 3 cases, we must begin searching from the left-most block.
        case LESS_THAN:
        case LESS_THAN_OR_EQUAL:
            index_scans[position].current_block_id = AM_files[fileDesc].lefterid;
            index_scans[position].record_n = -1;    //assign -1 to record_n to show that the first record of the block must be read
    }
    return position;
}

void *AM_FindNextEntry(int scanDesc)
{
    int records_in_block, i;
    void *from;
    if (AM_files[index_scans[scanDesc].am_file_pos].id < 0)
    {
        AM_errno = AME_FILE_NOT_OPEN;
        return NULL;
    }
    if (index_scans[scanDesc].am_file_pos < 0)
    {
        AM_errno = AME_SCAN_CLOSED;
        return NULL;
    }
    if (index_scans[scanDesc].current_block_id < 0) //if we have reached the end of the data (after the last block (right-most one)), there is no more records to look for, so exit.
    {
        AM_errno = AME_EOF;
        return NULL;
    }
    //read current block
    if (BF_ReadBlock(AM_files[index_scans[scanDesc].am_file_pos].id, index_scans[scanDesc].current_block_id, &from) < 0){
            AM_errno = AME_FILE_READ_FAILED;
            return NULL;
    }
    index_scans[scanDesc].record_n++;    //we want to read NEXT entry
    memcpy((void *)&records_in_block, from + 1 + sizeof(int), sizeof(int));    //read block's record counter
    if (index_scans[scanDesc].record_n == records_in_block) //if next record number exceeds block's record amount, then we must go to the next block
    {
        memcpy((void *)(&(index_scans[scanDesc].current_block_id)), from + 1, sizeof(int));    //read next block id
        if (index_scans[scanDesc].current_block_id < 0) //if we reached the last block (right-most one), there is no more records to look for, so exit.
        {
            //index_scans[scanDesc].record_n--;//this is used to counter previous increment (to avoid possible (overflow) bugs due to calling FindNextEntry many times after last record has been visited)
            AM_errno = AME_EOF;
            return NULL;
        }
        if (BF_ReadBlock(AM_files[index_scans[scanDesc].am_file_pos].id, index_scans[scanDesc].current_block_id, &from) < 0)    //open next block
        {
            AM_errno = AME_FILE_READ_FAILED;
            return NULL;
        }
        index_scans[scanDesc].record_n = 0;
    }
    switch (index_scans[scanDesc].scan_type)
    {    //pointer to current record = block pointer + 9 (1 for block type, 4 for next block id, 4 for counter) + the length of all the previous records (+ the length of the 1st field of the record, if we want to get the second field)
        case NOT_EQUAL:
            if (compare(index_scans[scanDesc].am_file_pos, from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)), (void *)(index_scans[scanDesc].search_value)))
                memcpy((void *)(index_scans[scanDesc].tempbuffer), (from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)) + (AM_files[index_scans[scanDesc].am_file_pos].attrLength1)), AM_files[index_scans[scanDesc].am_file_pos].attrLength2);
            else
            {    //if we find something equal to the search value, we must search for the next record that has a non-equal (greater) value in its key field
                i = index_scans[scanDesc].record_n;
                while ((i < records_in_block) &&  !compare(index_scans[scanDesc].am_file_pos, from + 1 + 2*sizeof(int) + (i * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)), (void *)(index_scans[scanDesc].search_value))) i++;
                while (i == records_in_block) //if we reach the end of the block, we must move to the next one and keep searching
                {
                    memcpy((void *)(&index_scans[scanDesc].current_block_id), from + 1, sizeof(int));    //read next block id
                    if (index_scans[scanDesc].current_block_id < 0) //if we reached the last block (right-most one), there is no more records to look for, so exit.
                    {
                        AM_errno = AME_EOF;
                        return NULL;
                    }
                    if (BF_ReadBlock(AM_files[index_scans[scanDesc].am_file_pos].id, index_scans[scanDesc].current_block_id, &from) < 0)    //open next block
                    {
                        AM_errno = AME_FILE_READ_FAILED;
                        return NULL;
                    }
                    memcpy((void *)&records_in_block, from + 1 + sizeof(int), sizeof(int));    //read block's record counter
                    //index_scans[scanDesc].record_n = 0;    //this line may not be necessary, added for security
                    i = 0;
                    while ((i < records_in_block) &&  !compare(index_scans[scanDesc].am_file_pos, from + 1 + 2*sizeof(int) + (i * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)), (void *)(index_scans[scanDesc].search_value))) i++;
                }
                index_scans[scanDesc].record_n = i;
                //next (valid) entry's position found, read it and proceed
                memcpy((void *)(index_scans[scanDesc].tempbuffer), (from + 1 + 2*sizeof(int) + (i * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)) + (AM_files[index_scans[scanDesc].am_file_pos].attrLength1)), AM_files[index_scans[scanDesc].am_file_pos].attrLength2);
            }
            break;
        case EQUAL:
            if (!compare(index_scans[scanDesc].am_file_pos, from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)), (void *)(index_scans[scanDesc].search_value)))
                memcpy((void *)(index_scans[scanDesc].tempbuffer), (from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)) + (AM_files[index_scans[scanDesc].am_file_pos].attrLength1)), AM_files[index_scans[scanDesc].am_file_pos].attrLength2);
            else
            {
                AM_errno = AME_EOF;
                return NULL;
            }
            break;
        case GREATER_THAN_OR_EQUAL:
            if (compare(index_scans[scanDesc].am_file_pos, from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)), (void *)(index_scans[scanDesc].search_value)) >= 0)
                memcpy((void *)(index_scans[scanDesc].tempbuffer), (from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)) + (AM_files[index_scans[scanDesc].am_file_pos].attrLength1)), AM_files[index_scans[scanDesc].am_file_pos].attrLength2);
            else
            {
                AM_errno = AME_EOF;
                return NULL;
            }
            break;
        case GREATER_THAN:
            if (compare(index_scans[scanDesc].am_file_pos, from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)), (void *)(index_scans[scanDesc].search_value)) > 0)
                memcpy((void *)(index_scans[scanDesc].tempbuffer), (from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)) + (AM_files[index_scans[scanDesc].am_file_pos].attrLength1)), AM_files[index_scans[scanDesc].am_file_pos].attrLength2);
            else
            {
                AM_errno = AME_EOF;
                return NULL;
            }
            break;
        case LESS_THAN:
            if (compare(index_scans[scanDesc].am_file_pos, from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)), (void *)(index_scans[scanDesc].search_value)) < 0)
                memcpy((void *)(index_scans[scanDesc].tempbuffer), (from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)) + (AM_files[index_scans[scanDesc].am_file_pos].attrLength1)), AM_files[index_scans[scanDesc].am_file_pos].attrLength2);
            else
            {
                AM_errno = AME_EOF;
                return NULL;
            }
            break;
        case LESS_THAN_OR_EQUAL:
            if (compare(index_scans[scanDesc].am_file_pos, from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)), (void *)(index_scans[scanDesc].search_value)) <= 0)
                memcpy((void *)(index_scans[scanDesc].tempbuffer), (from + 1 + 2*sizeof(int) + (index_scans[scanDesc].record_n * (AM_files[index_scans[scanDesc].am_file_pos].attrLength1 + AM_files[index_scans[scanDesc].am_file_pos].attrLength2)) + (AM_files[index_scans[scanDesc].am_file_pos].attrLength1)), AM_files[index_scans[scanDesc].am_file_pos].attrLength2);
            else
            {
                AM_errno = AME_EOF;
                return NULL;
            }
    }
    return (void *)(index_scans[scanDesc].tempbuffer);
}

int AM_CloseIndexScan(int scanDesc)
{
    if (index_scans[scanDesc].am_file_pos >= 0){
        free(index_scans[scanDesc].search_value);
        free(index_scans[scanDesc].tempbuffer);
        index_scans[scanDesc].am_file_pos = -1;
        return AME_OK;
    }
    AM_errno = AME_SCAN_CLOSED;
    return AME_SCAN_CLOSED;
}

void AM_PrintError(char *errString)
{
    char message[256];
    printf("\t%s\n", errString);
    sprintf(message, "---AM ERROR---:  ");
    switch (AM_errno)
    {
        case AME_WRITE_BLOCK_FAILED:
            sprintf(message+17,"Writing block failed.\n");
            BF_PrintError(message);
            break;
        case AME_ALLOCATE_BLOCK_FAILED:
            sprintf(message+17,"Allocating block failed.\n");
            BF_PrintError(message);
            break;
        case AME_GET_BLOCK_C_FAILED:
            sprintf(message+17,"Getting block counter failed\n");
            BF_PrintError(message);
            break;
        case AME_CREATE_FILE_FAILED:
            sprintf(message+17,"Creating block file failed\n");
            BF_PrintError(message);
            break;
        case AME_OPEN_FILE_FAILED:
            sprintf(message+17,"Opening block file failed\n");
            BF_PrintError(message);
            break;
        case AME_NO_SPACE:
            printf("%sError opening item: limit reached. Close another item and try again.\n", message);
            break;
        case AME_SCAN_OPEN:
            printf("%sScan(s) not closed. Close them to continue.\n", message);
            break;
        case AME_SCAN_CLOSED:
            printf("%sScan not exists or has closed.\n", message);
            break;
        case AME_FILE_READ_FAILED:
            sprintf(message + 17, "Reading from block file failed.\n");//8elei Sprintf <----------
            BF_PrintError(message);
            break;
        case AME_FILE_CR_FAIL:
            printf("%sFile Creation failed.\n", message);
            break;
        case AME_FILE_OP_FAIL:
            printf("%sOpening file failed.\n", message);
            break;
        case AME_DESTR_FAIL:
            printf("%sDestroying index failed.\n", message);
            break;
        case AME_CLOSE_FAIL:
            printf("%sClosing index failed.\n", message);
            break;
        case AME_FILE_NOT_OPEN:
            printf("%sIndex not opened.\n", message);
            break;
        case AME_MALLOC_FAILED:
            printf("%sMemory allocation failed.\n", message);
            break;
        case AME_EOF:
            printf("%sNo more data to be read.\n", message);
            break;
        case AME_OK:
            printf("%sNo errors.\n", message);
            break;
        default:
            printf("%sUndefined error.\n", message);
    }
}
