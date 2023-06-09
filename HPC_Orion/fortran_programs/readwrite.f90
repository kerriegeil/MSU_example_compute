PROGRAM readwrite
    IMPLICIT NONE
    
    INTEGER, PARAMETER :: nrows=3, ntimes=5
    INTEGER :: var1(nrows,ntimes)
    INTEGER :: arglen1,arglen2,isf,r
    CHARACTER(LEN=10) :: fmt
    CHARACTER(LEN=*), PARAMETER :: datafile="../inputs/dummy_data.txt", outpref="../results/variable1_", outsuf=".txt" 
    CHARACTER(LEN=:), ALLOCATABLE :: cid,csf,outfile
    
    PRINT*, "READING LINE ARGUMENTS"
    
    ! get array task id
    CALL GET_COMMAND_ARGUMENT(1,LENGTH=arglen1)
    ALLOCATE(CHARACTER(arglen1) :: cid)
    CALL GET_COMMAND_ARGUMENT(1,VALUE=cid)
    
    ! get scale factor
    CALL GET_COMMAND_ARGUMENT(2,LENGTH=arglen2)
    ALLOCATE(CHARACTER(arglen2) :: csf)
    CALL GET_COMMAND_ARGUMENT(2,VALUE=csf)
    READ(csf,'(i6)') isf         
    
    PRINT*,"READING DATA FILE"
    OPEN(99, file=datafile)
    DO r=1,nrows
        READ(99,*) var1(r,:)
    END DO
    
    PRINT*, "COMPUTING var1"
    var1=var1*isf
    
    PRINT*, "WRITING NEW DATA FILE"

    ALLOCATE(CHARACTER(LEN(outpref)+LEN(cid)+LEN(outsuf)) :: outfile)
    outfile=outpref//cid//outsuf

    OPEN(100,file=outfile)    
    WRITE(fmt,'(a,i1,a)') "(",ntimes,"i6)"
    DO r=1,nrows
        WRITE(100,fmt) var1(r,:)
    END DO
        
END PROGRAM    