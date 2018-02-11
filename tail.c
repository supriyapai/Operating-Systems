#include "types.h"
#include "stat.h"
#include "user.h"
#include "fcntl.h"

char buf [1024];

void tail (int fd, int num) {
  int n,i;
  int noLines;
  int printLines;
  int tempFile;
  tempFile = open ("tailTemp", O_CREATE | O_RDWR);
  while ((n = read(fd, buf, sizeof(buf))) > 0) {
    write (tempFile, buf, n);
    for (i = 0; i<n; i++) {
      if(buf[i] == '\n')
        noLines++;
    }
  }
  if (n < 0) {
    printf (1, "tail: read error \n");
    exit ();
  }
  if (noLines < num)
    printLines = 0;
  printLines = noLines - num;

  close (tempFile);
  tempFile = open ("tailTemp", 0);

  int counter = 0;
  while ((n = read(tempFile, buf, sizeof(buf))) > 0) {
    for ( i = 0; i<n; i++) {
      if (counter >= printLines)
        printf(1,"%c",buf[i]);
      if (buf[i] == '\n')
        counter++;
      }
    }
    close (tempFile);
    unlink("tailTemp");
}

int
main(int argc , char *argv[])
{
int fd;

if(argc <= 1)
{
   tail(0, 10);
}
if(argc == 2)
{
    if( argv[1][0] == '-')
    {
     int nol = atoi(++argv[1]);
     tail(0, nol);
    }
    else
    {
    if((fd = open(argv[1], 0)) < 0){
    printf(1, "tail: cannot open %s\n", argv[1]);
    }
    tail(fd, 10);
    }
}
if(argc > 2)
{
  if((fd = open(argv[2], 0)) < 0){
    printf(1, "tail: cannot open %s\n", argv[2]);
    exit();
  }
   int nol = atoi(++argv[1]);
   tail(fd, nol);
}

exit();
}
 

