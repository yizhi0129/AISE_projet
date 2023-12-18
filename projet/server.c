#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <unistd.h>
#include <pthread.h>
#include <ctype.h>


#define PORT 6379
#define BUFFER_SIZE 1024
//#define MAX_KEY_SIZE 256
#define HASH_TABLE_SIZE 1024

typedef struct node {
    char* key;
    void* value;
    size_t key_size;
    size_t value_size;
    struct node* next;
} node_t;

node_t* hashTable[HASH_TABLE_SIZE];

unsigned int hash(const char* key, size_t key_size) {
    unsigned long int value = 0;
    for (size_t i = 0; i < key_size; ++i) {
        value = value * 37 + *key++;
    }
    return value % HASH_TABLE_SIZE;
}

unsigned int hashBinary(const char* key, size_t len) {
    unsigned long int value = 0;
    for (size_t i = 0; i < len; ++i) {
        value = value * 37 + key[i];
    }
    return value % HASH_TABLE_SIZE;
}

void setKeyValue(const char* key, size_t key_size, const void* value, size_t value_size) {
    unsigned int index = hash(key, key_size);
    node_t* node = hashTable[index];

    while (node != NULL && (node->key_size != key_size || strncmp(node->key, key, key_size) != 0)) {
        node = node->next;
    }

    if (node != NULL) 
    {
        free(node->value);
        node->value = malloc(value_size);
        if (node->value == NULL) 
        {
            perror("Failed to allocate memory for value");
            return;
        }

        memcpy(node->value, value, value_size);
        node->value_size = value_size;
        return;
    }

    node = malloc(sizeof(node_t));
    if (node == NULL) {
        perror("Failed to allocate memory for node");
        return;
    }


    node->key = malloc(key_size);
    if (node->key == NULL) {
        perror("Failed to allocate memory for key");
        free(node);
        return;
    }
    
    memcpy(node->key, key, key_size);
    node->key_size = key_size;

    node->value = malloc(value_size);
    if (node->value == NULL) {
        perror("Failed to allocate memory for value");
        free(node->key);
        free(node);
        return;
    }

    memcpy(node->value, value, value_size);
    node->value_size = value_size;
    node->next = hashTable[index];
    hashTable[index] = node;
}

void* getKeyValue(const char* key, size_t key_size, size_t* value_size) {
    unsigned int index = hash(key, key_size);
    node_t* node = hashTable[index];

    while (node != NULL) {
        if (node->key_size == key_size && memcmp(node->key, key, key_size) == 0) {
            *value_size = node->value_size;
            return node->value;
        }
        node = node->next;
    }
    return NULL;
}

void delKey(const char* key, size_t key_size) {
    unsigned int index = hash(key, key_size);
    node_t* prev = NULL;
    node_t* node = hashTable[index];

    while (node != NULL && (node->key_size != key_size || memcmp(node->key, key, key_size) != 0)) {
        prev = node;
        node = node->next;
    }

    if (node == NULL) {
        return;
    }

    if (prev == NULL) {
        hashTable[index] = node->next;
    }
    else {
        prev->next = node->next;
    }

    free(node->key);
    free(node->value);
    node->key = NULL;
    node->value = NULL;
    free(node);
}

void processCommand(int clientSocket, char* command) 
{
    char response[BUFFER_SIZE];
    size_t value_size;
    size_t key_size;
    if (strncmp(command, "SET", 3) == 0) 
    {
        char key[BUFFER_SIZE];
        sscanf(command, "SET %s", key);
        key_size = strlen(key);
        char* value_str = strchr(command, ' ') + 1;
        if (strncmp(value_str, "0x", 2) == 0) 
        {
            // Convert hexadecimal string to binary data
            value_size = (strlen(value_str) - 2) / 2;
            void* value_bin = malloc(value_size);
            sscanf(value_str + 2, "%*2x%*2x%*2x%*2x%*2x%*2x%*2x%*2x", value_bin);
            setKeyValue(key, key_size, value_bin, value_size);
            free(value_bin);
        } 
        else 
        {
            // Set the value as a string
            value_size = strlen(value_str);
            setKeyValue(key, key_size, value_str, value_size);
        }
        snprintf(response, BUFFER_SIZE, "+OK\r\n");
    }
    else if (strncmp(command, "GET", 3) == 0) 
    {
        char key[BUFFER_SIZE];
        sscanf(command, "GET %s", key);
        key_size = strlen(key);
        void* value = getKeyValue(key, key_size, &value_size);

        if (value) 
        {
            // Check if the value is binary
            int is_binary = 0;
            for (size_t i = 0; i < value_size; ++i) 
            {
                if (!isprint(((char*)value)[i])) 
                {
                    is_binary = 1;
                    break;
                }
            }

            if (is_binary) 
            {
                // Print binary data as hexadecimal string
                snprintf(response, BUFFER_SIZE, "$");
                for (size_t i = 0; i < value_size; i++) 
                {
                    snprintf(response + strlen(response), BUFFER_SIZE - strlen(response), "%02x", ((unsigned char*)value)[i]);
                }
                snprintf(response + strlen(response), BUFFER_SIZE - strlen(response), "\r\n");
            }
            else 
            {
                snprintf(response, BUFFER_SIZE, "$%s\r\n", (char*)value);
            }
        }
        else 
        {
            snprintf(response, BUFFER_SIZE, "$-1\r\n");
        }
    }
    else if (strncmp(command, "DEL", 3) == 0) 
    {
        char key[BUFFER_SIZE];
        sscanf(command, "DEL %s", key);
        size_t key_size = strlen(key);
        delKey(key, key_size);
        snprintf(response, BUFFER_SIZE, "+OK\r\n");
    }
    else 
    {
        snprintf(response, BUFFER_SIZE, "-ERROR Unknown Command\r\n");
    }
    send(clientSocket, response, strlen(response), 0);
}

void* handleClient(void* arg) 
{
    int clientSocket = *(int*)arg;
    char buffer[BUFFER_SIZE];

    while (1) 
    {
        memset(buffer, 0, BUFFER_SIZE);
        ssize_t numBytes = recv(clientSocket, buffer, BUFFER_SIZE - 1, 0);

        if (numBytes <= 0) 
        {
            // The client has closed the connection or an error occurred
            if (numBytes == 0) {
                printf("Client disconnected\n");
            } else {
                perror("recv");
            }
            close(clientSocket);
            return NULL;
        }

        // Process the command and send the response
        char response[BUFFER_SIZE];
        processCommand(buffer, response);
        ssize_t sentBytes = send(clientSocket, response, strlen(response), 0);
        if (sentBytes < 0) 
        {
            perror("send");
            close(clientSocket);
            return NULL;
        }
    }
}

int main() 
{
    int serverSocket, *clientSocket;
    struct sockaddr_in serverAddr, clientAddr;
    socklen_t clientAddrLen = sizeof(clientAddr);
    pthread_t thread;

    serverSocket = socket(AF_INET, SOCK_STREAM, 0);
    if (serverSocket == -1) {
        perror("Failed to create socket");
        exit(EXIT_FAILURE);
    }

    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(PORT);
    serverAddr.sin_addr.s_addr = INADDR_ANY;

    if (bind(serverSocket, (struct sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        perror("Failed to bind to port");
        exit(EXIT_FAILURE);
    }

    if (listen(serverSocket, 5) < 0) {
        perror("Failed to listen on socket");
        exit(EXIT_FAILURE);
    }

    printf("Server is listening on port %d...\n", PORT);

    while (1) {
        clientSocket = malloc(sizeof(int));
        if (clientSocket == NULL) {
            perror("Failed to allocate memory for client socket");
            continue;
        }

        *clientSocket = accept(serverSocket, (struct sockaddr*)&clientAddr, &clientAddrLen);
        if (*clientSocket < 0) {
            perror("Failed to accept client connection");
            free(clientSocket);
            continue;
        }

        pthread_create(&thread, NULL, handleClient, (void*)clientSocket);
        pthread_detach(thread);
    }

    
    close(serverSocket);
    return 0;
}
